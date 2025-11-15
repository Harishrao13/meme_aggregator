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
const TOKEN_SET_KEY = `discover:${CHAIN}:tokens`;
const TOKEN_HASH_PREFIX = `token:`;
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
  console.log(`fetchLatestTokenProfiles -> ${url}`);
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
    console.log(`fetchTokenDetails -> calling local price API: ${localUrl}`);
    const localRes = await backoff(() =>
      axios.get(localUrl, { timeout: 10_000, validateStatus: null })
    ) as any;

    if (!localRes) {
      console.warn(`fetchTokenDetails -> no response from local price API for ${addr}`);
      return null;
    }

    if (localRes.status >= 200 && localRes.status < 300 && localRes.data) {
      console.log(`fetchTokenDetails -> received local data for ${addr}`);
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
 * This data is used for scoring.
 */
async function writeTokenPayload(addr: string, payload: AnyObject | null): Promise<void> {
  // The payload now comes from our local API, inside the 'data' property.
  const localData = payload?.data;
  
  if (!localData) {
    console.warn(`No data payload found for token ${addr}, cannot write to Redis.`);
    return;
  }

  const key = `${TOKEN_HASH_PREFIX}${addr}`;
  const flat: Record<string, string> = {};

  // Assuming localData has a structure similar to the old 'mainPair'
  // e.g., { baseToken: { ... }, priceUsd: "...", ... }
  flat.token_address = String(addr);
  if (localData?.baseToken?.name) flat.token_name = String(localData.baseToken.name);
  if (localData?.baseToken?.symbol) flat.token_ticker = String(localData.baseToken.symbol);
  if (localData?.priceUsd) flat.aggregated_price_usd = String(localData.priceUsd);
  if (localData?.liquidity?.usd) flat.liquidity_usd = String(localData.liquidity.usd);
  if (localData?.volume?.h24) flat.volume_usd = String(localData.volume.h24);
  flat.last_discovered_ts = String(Date.now());
  
  const pipeline = client.multi();
  pipeline.hset(key, flat);
  pipeline.expire(key, 60 * 60 * 24); // 24-hour expiry
  await pipeline.exec();
}

export async function runDiscoveryLoop({} = {}): Promise<void> {
  // console.log("Discovery loop starting. singleIteration=", singleIteration);
  while (true) {
    try {
      console.log("Discovery iteration start —", new Date().toISOString());

      // --- 1. DISCOVER ---
      const profiles = await fetchLatestTokenProfiles();
      console.log(" profiles fetched:", profiles.length);

      const discovered: string[] = [];
      for (const profile of profiles) {
        try {
          if (profile?.tokenAddress) discovered.push(String(profile.tokenAddress).toLowerCase());
        } catch {}
      }

      const unique = Array.from(new Set(discovered));
      console.log(" discovered unique tokens:", unique.length);

      // --- 2. ADD TO SET ---
      const added = await addToTokenSet(unique);
      console.log(" added to token set:", added);

      // --- 3. PREFETCH & SCORE (Optional) ---
      if (PREFETCH_TOKEN_DETAILS && unique.length > 0) {
        console.log(`Prefetching details for ${unique.length} tokens...`);
        const details = await prefetchDetailsForAddresses(unique);
        for (const d of details) {
          await writeTokenPayload(d.addr, d.payload);
        }
        console.log(`Prefetch complete.`);
      }

      // if (singleIteration) {
      //   console.log("singleIteration: exiting after one loop (debug)");
      //   break;
      // }

      console.log("Sleeping for DISCOVERY_INTERVAL_MS:", DISCOVERY_INTERVAL_MS);
      await sleep(DISCOVERY_INTERVAL_MS);
    } catch (err) {
      console.error("Discovery loop error:", err);
      await sleep(5000);
    }
  }
}

// --- Worker self-execution ---
const __filename = fileURLToPath(import.meta.url);
const isMain = process.argv[1] && resolve(process.argv[1]) === resolve(__filename);

console.log("Starting discovery.worker");
console.log(" NODE_ENV=", process.env.NODE_ENV ?? "<not set>");
console.log(" DEXSCREENER_BASE=", DEXSCREENER_BASE);
console.log(" CHAIN=", CHAIN);
console.log(" DISCOVERY_INTERVAL_MS=", DISCOVERY_INTERVAL_MS);
console.log(" REQUEST_INTERVAL_MS=", REQUEST_INTERVAL_MS);
console.log(" PREFETCH_TOKEN_DETAILS=", PREFETCH_TOKEN_DETAILS);
console.log(" PREFETCH_CONCURRENCY=", PREFETCH_CONCURRENCY);
console.log(" SINGLE_ITERATION=", SINGLE_ITERATION);

if (isMain) {
  runDiscoveryLoop().catch((err) => {
    console.error("Discovery worker fatal:", err);
    process.exit(1);
  });
}