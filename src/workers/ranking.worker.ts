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

// Weights
const W_RECENCY = Number(process.env.RANKER_W_RECENCY ?? 0.20);
const W_LIQ = Number(process.env.RANKER_W_LIQ ?? 0.33);
const W_VOL = Number(process.env.RANKER_W_VOL ?? 0.27);
const W_PCHG = Number(process.env.RANKER_W_PCHG ?? 0.12);
const W_TX = Number(process.env.RANKER_W_TX ?? 0.08);

// Cleanup / retention
const TOP_KEEP_COUNT = Number(process.env.RANKER_TOP_KEEP ?? 30);
const REMOVE_IF_NOT_TOP_MS = Number(process.env.RANKER_PRUNE_MS ?? 60 * 60 * 1000); // 1 hour
const NEWER_WINDOW_MS = Number(process.env.RANKER_NEWER_WINDOW_MS ?? 24 * 3600 * 1000); // 24h

// Aging config
export const AGING_INTERVAL_MS = Number(process.env.RANKER_AGING_INTERVAL_MS ?? 5 * 60 * 1000); // 5 minutes
export const AGING_FACTOR = Number(process.env.RANKER_AGING_FACTOR ?? 0.90); // 10% decay
export const MIN_SCORE_THRESHOLD = Number(process.env.RANKER_MIN_SCORE ?? 1);
export const MAX_AGE_CYCLES = Number(process.env.RANKER_MAX_AGE_CYCLES ?? 12);
export const LAST_AGING_KEY = `discover:${CHAIN}:last_aging_ts`;

// Helpers
function log1pSafe(x: number) { return Math.log1p(Math.max(0, Number(x) || 0)); }
function minMaxNormalize(values: number[]): number[] {
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
  const results = new Array<R | undefined>(items.length) as R[];
  let idx = 0;
  const runners = new Array(Math.min(concurrency, items.length)).fill(null).map(async () => {
    while (idx < items.length) {
      const i = idx++;
      try {
        const r = await fn(items[i]);
        results[i] = r as R;
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

const VOL_WINSOR_PCT = Number(process.env.RANKER_VOL_WINSOR_PCT ?? 0.95);
const VOL_COMPRESS_EXP = Number(process.env.RANKER_VOL_COMPRESS_EXP ?? 0.5);
const VOL_USE_RANK = (process.env.RANKER_VOL_USE_RANK ?? "false") === "true";

function winsorize(values: number[], pct: number) {
  if (!values.length) return values.slice();
  const sorted = values.slice().sort((a, b) => a - b);
  const idx = Math.floor((pct) * (sorted.length - 1));
  const cap = sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
  return values.map(v => (v > cap ? cap : v));
}

function rankNormalize(values: number[]) {
  // returns values in [0,1] where higher raw value => closer to 1
  if (!values.length) return [];
  // pairs of [value, originalIndex]
  const pairs = values.map((v, i) => ({ v, i }));
  pairs.sort((a, b) => a.v - b.v);
  const n = values.length;
  const out = new Array<number>(n);
  for (let r = 0; r < pairs.length; r++) {
    // rank from 0..n-1 -> normalized from 0..1
    out[pairs[r].i] = r / (n - 1 || 1);
  }
  return out;
}


/**
 * Score & write a single batch of addresses.
 * Adds a deterministic tiebreaker and a recency boost.
 * zset stores scaled integer scores (finalScore * 1e6) to maintain precision.
 */
async function scoreAndWriteBatch(addresses: string[], batchIndex?: number): Promise<void> {
  console.log(`scoreAndWriteBatch: starting batch ${batchIndex ?? "?"} size=${addresses.length}`);
  const start = Date.now();

  // Read existing token metadata for recency/last_updated_ts
  const metaPipe = client.multi();
  for (const addr of addresses) metaPipe.hget(`${TOKEN_HASH_PREFIX}${addr}`, "last_updated_ts");
  const metaRes = (await metaPipe.exec()) ?? [];
  const existingLastUpdated: number[] = metaRes.map((r: any) => {
    try {
      const val = Array.isArray(r) ? r[1] : r?.[1] ?? r;
      return val ? Number(val) : 0;
    } catch { return 0; }
  });

  const snapshots = await runWithConcurrency<string, AggregatedResult | null>(addresses, CONCURRENCY, fetchSnapshotSafe);

  const valid = snapshots
    .map((s, i) => ({ s, addr: addresses[i], prevLastUpdated: existingLastUpdated[i] }))
    .filter(x => x.s && (x.s as AggregatedResult).token_address) as { s: AggregatedResult; addr: string; prevLastUpdated: number }[];

  console.log(`scoreAndWriteBatch: batch ${batchIndex ?? "?"} valid snapshots=${valid.length}`);
  if (valid.length === 0) return;

  // Extract raw metrics used for scoring
  const liquidityLogs = valid.map(x => log1pSafe(x.s.scoreComponents?.liquidity_log ?? 0));
  const volumeLogs = valid.map(x => log1pSafe(x.s.scoreComponents?.volume_log ?? 0));
  const txLogs = valid.map(x => log1pSafe(x.s.scoreComponents?.txcount_log ?? 0));
  const pchgAbs = valid.map(x => Math.abs(x.s.scoreComponents?.price_change_abs ?? 0));

  const now = Date.now();
  const recencyRaw = valid.map((v, i) => {
    const prevTs = v.prevLastUpdated || 0;
    const snapTs = Number(v.s.last_updated_ts ?? 0);
    const sourceTs = prevTs || (snapTs ? snapTs : now);
    const age = Math.max(0, now - Number(sourceTs));
    return Math.max(0, 1 - age / NEWER_WINDOW_MS);
  });

  const normLiqu = minMaxNormalize(liquidityLogs);
  const normTx = minMaxNormalize(txLogs);
  const normPchg = minMaxNormalize(pchgAbs);
  let normVol: number[] = [];

  if (VOL_USE_RANK) {
    normVol = rankNormalize(volumeLogs).map(v => v);
  } else {
    // winsorize extreme values first to cap outliers
    const wins = winsorize(volumeLogs, VOL_WINSOR_PCT);
    const mm = minMaxNormalize(wins);
    const exp = VOL_COMPRESS_EXP > 0 ? VOL_COMPRESS_EXP : 1;
    normVol = mm.map(v => Math.pow(v, exp));
  }


  const liquiditySolVals = valid.map(v => Number(v.s.liquidity_sol ?? 0));
  const maxLiquiditySol = Math.max(...liquiditySolVals, 0);

  const pipeline = client.multi();
  const zaddEntries: Array<[number, string]> = [];

  for (let i = 0; i < valid.length; i++) {
    const snapshot = valid[i].s;
    const addr = snapshot.token_address ?? valid[i].addr;
    const tokenKey = `${TOKEN_HASH_PREFIX}${addr}`;

    const baseScore = (normLiqu[i] * W_LIQ) + (normVol[i] * W_VOL) + (normPchg[i] * W_PCHG) + (normTx[i] * W_TX);
    const recencyContribution = (recencyRaw[i] || 0) * W_RECENCY;

    const liquiditySolVal = liquiditySolVals[i] || 0;
    const tieNorm = maxLiquiditySol > 0 ? (Math.log1p(liquiditySolVal) / Math.log1p(maxLiquiditySol)) : 0;
    const tieContribution = tieNorm * 0.001;
    const finalScore = baseScore + recencyContribution + tieContribution;

    // scale to integer to preserve precision
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
      last_updated_ts: String(now),
      score_scaled: String(scoreScaled),
      score_raw: String(finalScore)
    };

    pipeline.del(tokenKey);
    pipeline.hset(tokenKey, flat);
    pipeline.expire(tokenKey, 60 * 5); // Short expiry, will be extended if it makes the top
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
}

/**
 * Aging: decay scores and remove stale tokens.
 * Runs periodically from the main loop.
 */
export async function applyAgingStrategy(): Promise<{ removed: number; updated: number }> {
  const now = Date.now();
  const last = await client.get(LAST_AGING_KEY);
  const lastTs = last ? Number(last) : 0;
  if (now - lastTs < AGING_INTERVAL_MS) return { removed: 0, updated: 0 };

  const pairs = await client.zrange(SCORE_ZSET_KEY, 0, -1, 'WITHSCORES');
  if (!pairs || pairs.length === 0) {
    await client.set(LAST_AGING_KEY, String(now));
    return { removed: 0, updated: 0 };
  }

  const addrs: string[] = [];
  const scores: number[] = [];
  for (let i = 0; i < pairs.length; i += 2) {
    addrs.push(pairs[i]);
    scores.push(Number(pairs[i + 1]));
  }

  // fetch metadata (last_top_ts and aging_cycle_count)
  const metaPipe = client.multi();
  for (const a of addrs) metaPipe.hget(`${TOKEN_HASH_PREFIX}${a}`, "last_top_ts");
  for (const a of addrs) metaPipe.hget(`${TOKEN_HASH_PREFIX}${a}`, "aging_cycle_count");
  const metaRes = (await metaPipe.exec()) ?? [];

  const metas: Array<{ last_top_ts: number; aging_cycle_count: number }> = [];
  for (let i = 0; i < addrs.length; i++) {
    const lastTopRaw = metaRes[i * 2] ? (Array.isArray(metaRes[i * 2]) ? metaRes[i * 2][1] : metaRes[i * 2]?.[1] ?? metaRes[i * 2]) : undefined;
    const agingRaw = metaRes[i * 2 + 1] ? (Array.isArray(metaRes[i * 2 + 1]) ? metaRes[i * 2 + 1][1] : metaRes[i * 2 + 1]?.[1] ?? metaRes[i * 2 + 1]) : undefined;
    const lastTop = lastTopRaw ? Number(lastTopRaw) : 0;
    const agingCycle = agingRaw ? Number(agingRaw) : 0;
    metas.push({ last_top_ts: lastTop, aging_cycle_count: agingCycle });
  }

  const pipe = client.multi();
  let removed = 0;
  let updated = 0;

  // Determine current top set for keep logic
  const curTop = await client.zrevrange(SCORE_ZSET_KEY, 0, TOP_KEEP_COUNT - 1);
  const curTopSet = new Set(curTop);

  for (let i = 0; i < addrs.length; i++) {
    const addr = addrs[i];
    const curScore = scores[i];
    const meta = metas[i];

    if (!meta) {
      pipe.zrem(SCORE_ZSET_KEY, addr);
      pipe.del(`${TOKEN_HASH_PREFIX}${addr}`);
      removed++;
      continue;
    }

    const newScore = Math.floor(curScore * AGING_FACTOR);
    const newAgingCycle = (meta.aging_cycle_count || 0) + 1;

    // decide removal
    const notInTop = !curTopSet.has(addr);
    const lastTopAge = meta.last_top_ts ? (Date.now() - meta.last_top_ts) : Number.MAX_SAFE_INTEGER;

    if (newScore < MIN_SCORE_THRESHOLD || newAgingCycle >= MAX_AGE_CYCLES || (notInTop && lastTopAge > REMOVE_IF_NOT_TOP_MS)) {
      pipe.zrem(SCORE_ZSET_KEY, addr);
      pipe.del(`${TOKEN_HASH_PREFIX}${addr}`);
      removed++;
    } else {
      pipe.zadd(SCORE_ZSET_KEY, newScore, addr);
      pipe.hset(`${TOKEN_HASH_PREFIX}${addr}`, 'aging_cycle_count', String(newAgingCycle), 'last_updated_ts', String(Date.now()));
      updated++;
    }
  }

  pipe.set(LAST_AGING_KEY, String(now));
  await pipe.exec();

  return { removed, updated };
}

/**
 * Publish new top list if it changed and stamp `last_top_ts` for tokens in the new top.
 */
async function publishTopChangeIfNeeded(prevTop: string[] | null, limit = 20): Promise<string[]> {
  const newTop = await client.zrevrange(SCORE_ZSET_KEY, 0, limit - 1);
  const changed = !prevTop || prevTop.length !== newTop.length || prevTop.some((a, i) => a !== newTop[i]);
  if (changed) {
    await client.publish(`discover:${CHAIN}:updated`, JSON.stringify({ top: newTop, ts: Date.now() }));

    const pipe = client.multi();
    const now = Date.now();
    for (const addr of newTop) {
      const tokenKey = `${TOKEN_HASH_PREFIX}${addr}`;
      pipe.hset(tokenKey, "last_top_ts", String(now));
      // Extend expiry for tokens that are in the top list
      pipe.expire(tokenKey, 60 * 60 * 24 * 7); // 7 days
    }
    try {
      await pipe.exec();
    } catch (e) {
      console.warn("publishTopChangeIfNeeded: failed to update last_top_ts", e);
    }
  }
  return newTop;
}

/**
 * Remove tokens that have been out of the top list for longer than the threshold.
 */
async function pruneOldNonTopTokens(currentTop: string[], allTokens: string[]): Promise<void> {
  if (!allTokens || allTokens.length === 0) return;
  const topSet = new Set(currentTop);
  const candidates = allTokens.filter(t => !topSet.has(t));
  if (candidates.length === 0) return;

  const pipe = client.multi();
  for (const addr of candidates) pipe.hget(`${TOKEN_HASH_PREFIX}${addr}`, "last_top_ts");
  const res = await pipe.exec();
  const now = Date.now();
  const toRemove: string[] = [];

  const resArr = res ?? [];

  for (let i = 0; i < candidates.length; i++) {
    try {
      const r = resArr[i];
      const val = Array.isArray(r) ? r[1] : r?.[1] ?? r;
      const lastTop = val ? Number(val) : 0;
      // Remove if it was in the top once, but hasn't been for a while
      if (lastTop > 0 && now - lastTop > REMOVE_IF_NOT_TOP_MS) toRemove.push(candidates[i]);
    } catch (e) {
      console.warn("pruneOldNonTopTokens: parse error", e);
    }
  }

  if (toRemove.length) {
    const pipe2 = client.multi();
    for (const addr of toRemove) {
      pipe2.srem(TOKEN_SET_KEY, addr);
      pipe2.del(`${TOKEN_HASH_PREFIX}${addr}`);
      pipe2.zrem(SCORE_ZSET_KEY, addr);
    }
    try {
      await pipe2.exec();
    } catch (e) {
      console.warn("pruneOldNonTopTokens: failed to remove tokens", e);
    }
  }
}

/**
 * Single-run ranking pass.
 */
export async function runRankingOnce(limitPublish = 20): Promise<string[]> {
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
export async function runRankingLoop(): Promise<void> {
  console.log("runRankingLoop: starting persistent ranking loop");
  let prevTop: string[] | null = null;
  let lastAgingRun = 0;
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

      // run aging if it's been long enough
      const now = Date.now();
      if (now - lastAgingRun >= AGING_INTERVAL_MS) {
        try {
          await applyAgingStrategy();
        } catch (e) {
          console.warn("runRankingLoop: aging failed", e);
        }
        lastAgingRun = Date.now();
      }

      try {
        await pruneOldNonTopTokens(prevTop ?? [], allTokens);
      } catch (e) {
        console.warn("runRankingLoop: prune failed", e);
      }

      console.log(`runRankingLoop: sleeping ${RUN_INTERVAL_MS}ms before next pass`);
      await new Promise(r => setTimeout(r, RUN_INTERVAL_MS));
    } catch (err) {
      console.error("runRankingLoop: error in loop:", err);
      console.log("runRankingLoop: sleeping 5000ms after error");
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

export async function startRanker(): Promise<void> {
  console.log("Starting ranker.worker");
  console.log(" NODE_ENV=", process.env.NODE_ENV ?? "<not set>");
  console.log(" CHAIN=", CHAIN);
  console.log(" BATCH_SIZE=", BATCH_SIZE);
  console.log(" CONCURRENCY=", CONCURRENCY);
  console.log(" RUN_INTERVAL_MS=", RUN_INTERVAL_MS);
  console.log(" SCORE_ZSET_KEY=", SCORE_ZSET_KEY);
  console.log(" AGING_INTERVAL_MS=", AGING_INTERVAL_MS);
  console.log(" AGING_FACTOR=", AGING_FACTOR);

  try {
    await runRankingLoop();
  } catch (err) {
    console.error("Ranking worker fatal:", err);
    process.exit(1);
  }
}

// Direct execution
const __filename = fileURLToPath(import.meta.url);
const isMain = resolve(process.argv[1] || "") === resolve(__filename);

if (isMain) startRanker();