import axios from "axios";
import type { Redis } from "ioredis";
import redis from "../utils/redisClient.js";
import dotenv from "dotenv";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";

dotenv.config();
const client = redis as unknown as Redis;

const DEXSCREENER_BASE = (process.env.DEXSCREENER_BASE ?? "https://api.dexscreener.com").replace(/\/+$/, "");
const CHAIN = process.env.DISCOVER_CHAIN ?? "solana";
export const TOKEN_SET_KEY = `discover:${CHAIN}:tokens`;
export const TOKEN_HASH_PREFIX = `token:`;
const REQUEST_INTERVAL_MS = Number(process.env.DISCOVERY_REQUEST_INTERVAL_MS ?? 350);
const DISCOVERY_INTERVAL_MS = Number(process.env.DISCOVERY_INTERVAL_MS ?? 30_000);
const PREFETCH_TOKEN_DETAILS = (process.env.DISCOVERY_PREFETCH ?? "false") === "true";
const PREFETCH_CONCURRENCY = Number(process.env.DISCOVERY_PREFETCH_CONCURRENCY ?? 6);

const PROFILES_PATH = '/token-profiles/latest/v1';

const SINGLE_ITERATION = (process.env.DISCOVERY_SINGLE_ITERATION ?? "false") === "true";

function sleep(ms = 0) {
  return new Promise((r) => setTimeout(r, ms));
}

type AnyObject = Record<string, any>;

async function backoff<T>(fn: () => Promise<T>, retries = 3, delay = 300): Promise<T> {
  let attempt = 0;
  while (true) {
    try {
      if (attempt > 0) console.warn(`backoff: attempt ${attempt + 1}`);
      return await fn();
    } catch (err: any) {
      console.error(`backoff: attempt ${attempt + 1} failed:`, err?.message ?? err);
      if (attempt >= retries) {
        console.error("backoff: out of retries, throwing");
        throw err;
      }
      const wait = delay * Math.pow(2, attempt);
      console.log(`backoff: sleeping ${wait}ms before retry`);
      await sleep(wait);
      attempt += 1;
    }
  }
}

/**
 * Fetches the latest token profiles from the discovery endpoint.
 */
async function fetchLatestTokenProfiles(): Promise<AnyObject[]> {
  const url = `${DEXSCREENER_BASE}${PROFILES_PATH}`;
  const res = await backoff(() => axios.get(url, { timeout: 10_000, validateStatus: null })) as any;
  if (!res) return [];
  return Array.isArray(res.data) ? res.data : [];
}

/**
 * Fetches detailed financial data from the local price API.
 */
async function fetchTokenDetails(addr: string): Promise<AnyObject | null> {
  const localBase = (process.env.PRICE_API_BASE ?? "http://localhost:8080").replace(/\/+$/, "");
  const localUrl = `${localBase}/price/test?tokenAddress=${encodeURIComponent(addr)}`;

  try {
    const localRes = await backoff(() =>
      axios.get(localUrl, { timeout: 10_000, validateStatus: null })
    ) as any;

    if (!localRes) {
      console.warn(`fetchTokenDetails -> no response from local price API for ${addr}`);
      return null;
    }

    if (localRes.status >= 200 && localRes.status < 300 && localRes.data) {
      // Wrap the response in the expected { data: ... } structure
      return { data: localRes.data };
    }

    console.warn(`fetchTokenDetails -> local price API returned status ${localRes.status} for ${addr}`);
    return null;
  } catch (err: any) {
    console.error(`fetchTokenDetails -> local price API failed for ${addr}:`, err?.message ?? err);
    return null;
  }
}

async function prefetchDetailsForAddresses(addresses: string[]): Promise<Array<{ addr: string; payload: AnyObject | null }>> {
  if (!addresses || addresses.length === 0) return [];
  const out: Array<{ addr: string; payload: AnyObject | null }> = [];
  let i = 0;
  const workers = new Array(Math.min(PREFETCH_CONCURRENCY || 1, addresses.length)).fill(null).map(async () => {
    while (true) {
      const idx = i++;
      if (idx >= addresses.length) break;
      const addr = addresses[idx];
      try {
        const payload = await fetchTokenDetails(addr);
        out.push({ addr, payload });
      } catch (err) {
        out.push({ addr, payload: null });
      }
      await sleep(REQUEST_INTERVAL_MS);
    }
  });
  await Promise.all(workers);
  return out;
}

async function addToTokenSet(addrs: string[]): Promise<number> {
  if (!addrs || addrs.length === 0) return 0;
  const pipeline = client.multi();
  for (const a of addrs) pipeline.sadd(TOKEN_SET_KEY, a);
  const exec = await pipeline.exec();
  let added = 0;
  if (Array.isArray(exec)) {
    for (const item of exec) {
      if (Array.isArray(item) && item.length === 2) {
        const val = item[1];
        if (typeof val === "number") added += val;
      } else if (typeof item === "number") {
        added += item;
      }
    }
  }
  return added;
}


/**
 * Writes the financial payload (from fetchTokenDetails) to a Redis Hash.
 * Supports multiple input shapes (aggregator result, price API sample, etc.)
 */
async function writeTokenPayload(addr: string, payload: AnyObject | null): Promise<void> {
  const localData = payload?.data ?? payload; // accept wrapped { data: ... } or raw object
  if (!localData) {
    console.warn(`No data payload found for token ${addr}, cannot write to Redis.`);
    return;
  }

  const key = `${TOKEN_HASH_PREFIX}${addr}`;
  const flat: Record<string, string> = {};

  // Basic identity
  flat.token_address = String(addr ?? localData.token_address ?? localData.address ?? "");

  // Names / ticker
  if (localData.token_name ?? localData.name ?? localData.baseToken?.name) {
    flat.token_name = String(localData.token_name ?? localData.name ?? localData.baseToken?.name);
  }
  if (localData.token_ticker ?? localData.ticker ?? localData.baseToken?.symbol ?? localData.symbol) {
    flat.token_ticker = String(localData.token_ticker ?? localData.ticker ?? localData.baseToken?.symbol ?? localData.symbol);
  }

  // Price fields (SOL & USD). Accept many variants.
  const priceSolCandidates = localData.price_sol ?? localData.priceSol ?? localData.aggregated_price_sol ?? localData.priceNative ?? localData.priceNative ?? localData.price_sol;
  const aggregatedPriceUsdCandidates = localData.aggregated_price_usd ?? localData.price_usd ?? localData.aggregatedPriceUsd ?? localData.priceUsd ?? localData.aggregated_price_usd;
  const aggregatedPriceSolCandidates = localData.aggregated_price_sol ?? localData.aggregatedPriceSol ?? localData.price_sol ?? localData.priceSol;

  if (priceSolCandidates !== undefined && priceSolCandidates !== null) {
    const v = Number(priceSolCandidates);
    if (Number.isFinite(v)) flat.price_sol = String(v);
  }

  if (aggregatedPriceUsdCandidates !== undefined && aggregatedPriceUsdCandidates !== null) {
    const v = Number(aggregatedPriceUsdCandidates);
    if (Number.isFinite(v)) flat.aggregated_price_usd = String(v);
  }

  if (aggregatedPriceSolCandidates !== undefined && aggregatedPriceSolCandidates !== null) {
    const v = Number(aggregatedPriceSolCandidates);
    if (Number.isFinite(v)) flat.aggregated_price_sol = String(v);
  }

  // Liquidity (SOL & USD)
  const liquiditySolCandidates = localData.liquidity_sol ?? localData.liquiditySol ?? localData.liquidity?.sol ?? localData.liquidity?.native ?? localData.liquidity?.sol ?? localData.liquidity_sol;
  const liquidityUsdCandidates = localData.liquidity_usd ?? localData.liquidityUsd ?? localData.liquidity?.usd;

  if (liquiditySolCandidates !== undefined && liquiditySolCandidates !== null) {
    const v = Number(liquiditySolCandidates);
    if (Number.isFinite(v)) flat.liquidity_sol = String(v);
  }
  if (liquidityUsdCandidates !== undefined && liquidityUsdCandidates !== null) {
    const v = Number(liquidityUsdCandidates);
    if (Number.isFinite(v)) flat.liquidity_usd = String(v);
  }

  // Volume (prefer USD if present, also store SOL)
  const volumeUsdCandidates = localData.volume_usd ?? localData.volumeUsd ?? localData.volume?.h24 ?? localData.volume24h ?? localData.volume_usd;
  const volumeSolCandidates = localData.volume_sol ?? localData.volumeSol ?? localData.volume_sol ?? localData.volume?.sol ?? localData.volume?.native;

  if (volumeUsdCandidates !== undefined && volumeUsdCandidates !== null) {
    const v = Number(volumeUsdCandidates);
    if (Number.isFinite(v)) flat.volume_usd = String(v);
  }
  if (volumeSolCandidates !== undefined && volumeSolCandidates !== null) {
    const v = Number(volumeSolCandidates);
    if (Number.isFinite(v)) flat.volume_sol = String(v);
  }

  // Transaction count
  const txCandidates = localData.transaction_count ?? localData.tx_count ?? localData.txns ?? localData.transactionCount;
  if (txCandidates !== undefined && txCandidates !== null) {
    const v = Number(txCandidates);
    if (Number.isFinite(v)) flat.transaction_count = String(Math.floor(v));
  }

  // Market cap if present
  const marketCapCandidates = localData.market_cap_usd ?? localData.marketCapUsd ?? localData.market_cap;
  if (marketCapCandidates !== undefined && marketCapCandidates !== null) {
    const v = Number(marketCapCandidates);
    if (Number.isFinite(v)) flat.market_cap_usd = String(v);
  }

  // Protocol / source
  if (localData.protocol ?? localData.protocolName ?? localData.source) {
    flat.protocol = String(localData.protocol ?? localData.protocolName ?? localData.source);
  }

  // Best pool: stringify entire object for debugging / reuse
  if (localData.best_pool ?? localData.bestPool ?? localData.best_pool) {
    try {
      flat.best_pool = JSON.stringify(localData.best_pool ?? localData.bestPool);
    } catch {
      flat.best_pool = String(localData.best_pool ?? localData.bestPool);
    }
    // Also copy common nested fields for quick access if present
    try {
      const bp = localData.best_pool ?? localData.bestPool;
      if (bp?.dexId) flat.best_pool_dex = String(bp.dexId);
      if (bp?.pairAddress) flat.best_pool_pair = String(bp.pairAddress);
      if (bp?.priceUsd !== undefined) {
        const v = Number(bp.priceUsd);
        if (Number.isFinite(v)) flat.best_pool_price_usd = String(v);
      }
      if (bp?.liquidityUsd !== undefined) {
        const v = Number(bp.liquidityUsd);
        if (Number.isFinite(v)) flat.best_pool_liquidity_usd = String(v);
      }
    } catch (e) {
      // ignore nested extraction failures
    }
  }

  // Cache hint if any
  if (localData._cache !== undefined && localData._cache !== null) {
    flat._cache = String(localData._cache);
  }

  // Price change fields: accept many names and normalize percent -> decimal if necessary
  const raw24 = localData.price_change_24h ?? localData.priceChange24h ?? localData.price_change_percent_24h ?? localData.change_24h ?? localData.change24h ?? localData.price_change;
  const raw1 = localData.price_change_1h ?? localData.priceChange1h ?? localData.change_1h ?? localData.change1h ?? (localData.priceChange?.h1 ?? null);

  function normalizeDelta(raw: unknown): number | null {
    if (raw === null || raw === undefined) return null;
    const n = Number(raw);
    if (!Number.isFinite(n)) return null;
    // heuristic: if the magnitude looks like a percent (>= 3) and >1, divide by 100
    if (Math.abs(n) > 3 && n > 1) return n / 100;
    return n;
  }

  const d24 = normalizeDelta(raw24);
  const d1 = normalizeDelta(raw1);
  if (d24 !== null) flat.price_change_24h = String(d24);
  if (d1 !== null) flat.price_change_1h = String(d1);

  // last discovered / updated timestamp
  flat.last_discovered_ts = String(Date.now());

  // Persist to Redis
  const pipeline = client.multi();
  pipeline.hset(key, flat);
  pipeline.expire(key, 60 * 60 * 24); // 24-hour expiry
  await pipeline.exec();
}


export async function runDiscoveryLoop({} = {}): Promise<void> {
  while (true) {
    try {
      console.log("Discovery iteration start —", new Date().toISOString());

      const profiles = await fetchLatestTokenProfiles();
      console.log(" profiles fetched:", profiles.length);

      const discovered: string[] = [];
      for (const profile of profiles) {
        try {
          const profileChain = (profile?.chainId ?? profile?.chain ?? "");
          if (!profileChain) continue;

          if (String(profileChain).toLowerCase() !== String(CHAIN).toLowerCase()) continue;

          if (profile?.tokenAddress) {
            discovered.push(String(profile.tokenAddress).toLowerCase());
          }
        } catch (e) {
          // ignore malformed entries
        }
      }


      const unique = Array.from(new Set(discovered));
      console.log(" discovered unique tokens:", unique.length);

      const added = await addToTokenSet(unique);
      console.log(" added to token set:", added);

      if (PREFETCH_TOKEN_DETAILS && unique.length > 0) {
        console.log(`Prefetching details for ${unique.length} tokens...`);
        const details = await prefetchDetailsForAddresses(unique);
        for (const d of details) {
          await writeTokenPayload(d.addr, d.payload);
        }
        console.log(`Prefetch complete.`);
      }

      if (SINGLE_ITERATION) {
        console.log("SINGLE_ITERATION is true: exiting after one loop.");
        break;
      }

      console.log("Sleeping for DISCOVERY_INTERVAL_MS:", DISCOVERY_INTERVAL_MS);
      await sleep(DISCOVERY_INTERVAL_MS);
    } catch (err) {
      console.error("Discovery loop error:", err);
      await sleep(5000);
    }
  }
}

export async function startDiscovery() {
  console.log("Starting discovery.worker");
  console.log(" NODE_ENV=", process.env.NODE_ENV ?? "<not set>");
  console.log(" DEXSCREENER_BASE=", DEXSCREENER_BASE);
  console.log(" CHAIN=", CHAIN);
  console.log(" DISCOVERY_INTERVAL_MS=", DISCOVERY_INTERVAL_MS);
  console.log(" REQUEST_INTERVAL_MS=", REQUEST_INTERVAL_MS);
  console.log(" PREFETCH_TOKEN_DETAILS=", PREFETCH_TOKEN_DETAILS);
  console.log(" PREFETCH_CONCURRENCY=", PREFETCH_CONCURRENCY);
  console.log(" SINGLE_ITERATION=", SINGLE_ITERATION);

  try {
    await runDiscoveryLoop();
  } catch (err) {
    console.error("Discovery worker fatal:", err);
    process.exit(1);
  }
}

//Direct execution
const __filename = fileURLToPath(import.meta.url);
const isMain = resolve(process.argv[1] || "") === resolve(__filename);

if (isMain) {
  startDiscovery();
}