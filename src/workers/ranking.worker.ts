import { fileURLToPath } from "url";
import { resolve } from "path";
import type { Redis } from "ioredis";
import redis from "../utils/redisClient.js";
import { AggregatorService } from "../services/aggregator.service.js";
import type { AggregatedResult } from "../models/AggregatedResult.js";

const client = redis as unknown as Redis;

// Config / keys
export const CHAIN = "solana";
export const TOKEN_SET_KEY = `discover:${CHAIN}:tokens`;
export const SCORE_ZSET_KEY = `discover:${CHAIN}:score`;
export const TOKEN_HASH_PREFIX = `token:`;
const BATCH_SIZE = Number(process.env.RANKER_BATCH_SIZE ?? 100);
const CONCURRENCY = Number(process.env.RANKER_CONCURRENCY ?? 8);
const RUN_INTERVAL_MS = Number(process.env.RANKER_INTERVAL_MS ?? 10_000);
const W_LIQ = 0.40, W_VOL = 0.35, W_PCHG = 0.15, W_TX = 0.10;

// Helpers
function log1pSafe(x: number) { return Math.log1p(Math.max(0, Number(x) || 0)); }
function minMaxNormalize(values: number[]) {
  if (!values.length) return [];
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (max === min) return values.map(() => 0.5);
  return values.map(v => (v - min) / (max - min));
}
function chunk<T>(arr: T[], size = 100): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}
async function runWithConcurrency<T, R>(items: T[], concurrency: number, fn: (t: T) => Promise<R>) {
  const results: R[] = [];
  let idx = 0;
  const runners = new Array(Math.min(concurrency, items.length)).fill(null).map(async () => {
    while (idx < items.length) {
      const i = idx++;
      try {
        const r = await fn(items[i]);
        results[i] = r;
      } catch (err) {
        console.error("runWithConcurrency: item handler error:", err);
        results[i] = undefined as unknown as R;
      }
    }
  });
  await Promise.all(runners);
  return results;
}

async function fetchSnapshotSafe(addr: string): Promise<AggregatedResult | null> {
  try {
    const snap = await AggregatorService.liquidityWeightedAggregate(addr, null);
    return snap;
  } catch (err) {
    console.error(`fetchSnapshotSafe: snapshot failed for ${addr}`, err);
    return null;
  }
}

/**
 * Score & write a single batch of addresses.
 * Adds a tiny deterministic tiebreaker based on liquidity_sol to avoid identical scores.
 */
async function scoreAndWriteBatch(addresses: string[], batchIndex?: number) {
  console.log(`scoreAndWriteBatch: starting batch ${batchIndex ?? "?"} size=${addresses.length}`);
  const start = Date.now();

  const snapshots = await runWithConcurrency<string, AggregatedResult | null>(
    addresses,
    CONCURRENCY,
    fetchSnapshotSafe
  );

  const valid = snapshots
    .map((s, i) => ({ s, addr: addresses[i] }))
    .filter(x => x.s && x.s.token_address);

  console.log(`scoreAndWriteBatch: batch ${batchIndex ?? "?"} valid snapshots=${valid.length}`);

  if (valid.length === 0) {
    console.log(`scoreAndWriteBatch: batch ${batchIndex ?? "?"} no valid snapshots, skipping`);
    return;
  }

  // Extract raw metrics used for scoring
  const liquidityLogs = valid.map(x => log1pSafe((x.s as AggregatedResult).scoreComponents?.liquidity_log ?? 0));
  const volumeLogs = valid.map(x => log1pSafe((x.s as AggregatedResult).scoreComponents?.volume_log ?? 0));
  const txLogs = valid.map(x => log1pSafe((x.s as AggregatedResult).scoreComponents?.txcount_log ?? 0));
  const pchgAbs = valid.map(x => Math.abs((x.s as AggregatedResult).scoreComponents?.price_change_abs ?? 0));

  // Debug: print a few sample scoreComponents (first 3)
  console.debug(`scoreAndWriteBatch: sample scoreComponents (first 3):`, valid.slice(0, 3).map(v => v.s?.scoreComponents ?? {}));

  const normLiqu = minMaxNormalize(liquidityLogs);
  const normVol = minMaxNormalize(volumeLogs);
  const normTx = minMaxNormalize(txLogs);
  const normPchg = minMaxNormalize(pchgAbs);

  // Precompute liquidity_sol values and max for tie-breaker
  const liquiditySolVals = valid.map(v => Number((v.s as AggregatedResult).liquidity_sol ?? 0));
  const maxLiquiditySol = Math.max(...liquiditySolVals, 0);
  console.log(`scoreAndWriteBatch: batch ${batchIndex ?? "?"} maxLiquiditySol=${maxLiquiditySol} (samples: ${liquiditySolVals.slice(0,3).join(",")})`);

  const pipeline = client.multi();
  const zaddEntries: Array<[number, string]> = [];

  for (let i = 0; i < valid.length; i++) {
    const snapshot = valid[i].s as AggregatedResult;
    const addr = snapshot.token_address ?? valid[i].addr;
    const tokenKey = `${TOKEN_HASH_PREFIX}${addr}`;

    // Base score
    const baseScore = (normLiqu[i] * W_LIQ) + (normVol[i] * W_VOL) + (normPchg[i] * W_PCHG) + (normTx[i] * W_TX);

    // Tiebreaker: small deterministic bump from liquidity magnitude.
    // Normalize via log1p to compress scale; contributes at most 0.001 to score.
    const liquiditySolVal = liquiditySolVals[i] || 0;
    const tieNorm = maxLiquiditySol > 0 ? (Math.log1p(liquiditySolVal) / Math.log1p(maxLiquiditySol)) : 0;
    const tieContribution = tieNorm * 0.001; // tiny bump
    const finalScore = baseScore + tieContribution;

    const scoreScaled = Math.round(finalScore * 1e6);

    const flat: Record<string, string> = {
      token_address: String(snapshot.token_address ?? addr),
      token_name: String(snapshot.token_name ?? ""),
      token_ticker: String(snapshot.token_ticker ?? ""),
      price_sol: String(snapshot.price_sol ?? 0),
      aggregated_price_usd: String(snapshot.aggregated_price_usd ?? 0),
      liquidity_sol: String(snapshot.liquidity_sol ?? 0),
      volume_sol: String(snapshot.volume_sol ?? 0),
      transaction_count: String(snapshot.transaction_count ?? 0),
      price_1hr_change: String(snapshot.price_1hr_change ?? 0),
      price_24hr_change: String(snapshot.price_24hr_change ?? 0),
      price_7d_change: String(snapshot.price_7d_change ?? 0),
      last_updated_ts: String(Date.now()),
      score_scaled: String(scoreScaled),
      score_raw: String(finalScore)
    };

    pipeline.del(tokenKey);
    pipeline.hset(tokenKey, flat);
    pipeline.expire(tokenKey, 60 * 5);
    zaddEntries.push([scoreScaled, addr]);
  }

  await pipeline.exec();

  if (zaddEntries.length) {
    const flatArgs: Array<string | number> = [];
    for (const [sc, member] of zaddEntries) {
      flatArgs.push(sc, member);
    }
    await client.zadd(SCORE_ZSET_KEY, ...flatArgs as any);
  }

  const took = Date.now() - start;
  console.log(`scoreAndWriteBatch: finished batch ${batchIndex ?? "?"} wrote=${zaddEntries.length} took=${took}ms`);
}

/**
 * Publish new top if changed.
 */
async function publishTopChangeIfNeeded(prevTop: string[] | null, limit = 20) {
  const newTop = await client.zrevrange(SCORE_ZSET_KEY, 0, limit - 1);
  const changed = !prevTop || prevTop.length !== newTop.length || prevTop.some((a, i) => a !== newTop[i]);
  if (changed) {
    console.log(`publishTopChangeIfNeeded: top changed (prev ${prevTop?.length ?? 0} -> new ${newTop.length}), publishing`);
    await client.publish(`discover:${CHAIN}:updated`, JSON.stringify({ top: newTop, ts: Date.now() }));
  } else {
    console.log("publishTopChangeIfNeeded: top unchanged");
  }
  return newTop;
}

/**
 * Single-run ranking pass.
 */
export async function runRankingOnce(limitPublish = 20) {
  console.log("runRankingOnce: starting one-shot ranking pass");
  const allTokens = await client.smembers(TOKEN_SET_KEY);
  console.log(`runRankingOnce: token set size=${allTokens?.length ?? 0}`);
  if (!allTokens || allTokens.length === 0) return [];

  const batches = chunk(allTokens, BATCH_SIZE);
  for (let bi = 0; bi < batches.length; bi++) {
    const b = batches[bi];
    try {
      console.log(`runRankingOnce: processing batch ${bi + 1}/${batches.length}`);
      await scoreAndWriteBatch(b, bi + 1);
    } catch (err) {
      console.error(`runRankingOnce: scoreAndWriteBatch error on batch ${bi + 1}`, err);
    }
    await new Promise(r => setTimeout(r, 250));
  }

  const top = await client.zrevrange(SCORE_ZSET_KEY, 0, limitPublish - 1);
  console.log(`runRankingOnce: completed. published top count=${top.length}`);
  await client.publish(`discover:${CHAIN}:updated`, JSON.stringify({ top, ts: Date.now() }));
  return top;
}

/**
 * Long-running loop
 */
export async function runRankingLoop() {
  console.log("runRankingLoop: starting persistent ranking loop");
  let prevTop: string[] | null = null;
  while (true) {
    try {
      console.log("runRankingLoop: reading token set");
      const allTokens = await client.smembers(TOKEN_SET_KEY);
      console.log(`runRankingLoop: token set size=${allTokens?.length ?? 0}`);
      if (!allTokens || allTokens.length === 0) {
        console.log(`runRankingLoop: no tokens, sleeping ${RUN_INTERVAL_MS}ms`);
        await new Promise(r => setTimeout(r, RUN_INTERVAL_MS));
        continue;
      }
      const batches = chunk(allTokens, BATCH_SIZE);
      for (let bi = 0; bi < batches.length; bi++) {
        const b = batches[bi];
        console.log(`runRankingLoop: processing batch ${bi + 1}/${batches.length} (size=${b.length})`);
        await scoreAndWriteBatch(b, bi + 1);
        await new Promise(r => setTimeout(r, 250));
      }
      prevTop = await publishTopChangeIfNeeded(prevTop, 20);
      console.log(`runRankingLoop: sleeping ${RUN_INTERVAL_MS}ms before next pass`);
      await new Promise(r => setTimeout(r, RUN_INTERVAL_MS));
    } catch (err) {
      console.error("runRankingLoop: error in loop:", err);
      console.log("runRankingLoop: sleeping 5000ms after error");
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

// --- Worker self-execution ---
const __filename = fileURLToPath(import.meta.url);
const isMain = process.argv[1] && resolve(process.argv[1]) === resolve(__filename);

console.log("Starting ranker.worker");
console.log(" NODE_ENV=", process.env.NODE_ENV ?? "<not set>");
console.log(" CHAIN=", CHAIN);
console.log(" BATCH_SIZE=", BATCH_SIZE);
console.log(" CONCURRENCY=", CONCURRENCY);
console.log(" RUN_INTERVAL_MS=", RUN_INTERVAL_MS);
console.log(" SCORE_ZSET_KEY=", SCORE_ZSET_KEY);

if (isMain) {
  runRankingLoop().catch((err) => {
    console.error("Ranking worker fatal:", err);
    process.exit(1);
  });
}
