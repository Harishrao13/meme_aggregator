import { fileURLToPath } from "url";
import { resolve } from "path";
import type { Redis } from "ioredis";
import redis from "../utils/redisClient.js"; 
import { AggregatorService } from "../services/aggregator.service.js"; 
import type { AggregatedResult } from "../models/AggregatedResult.js"; 

const client = redis as unknown as Redis;
export const CHAIN = "solana";
export const TOKEN_HASH_PREFIX = `token:`;
const CONCURRENCY = Number(process.env.RANKER_CONCURRENCY ?? 8);

// === DELTA WORKER CONFIG ===
const DELTA_INTERVAL_MS = Number(process.env.DELTA_INTERVAL_MS ?? 8000);
const RANKING_CHANGED_CHANNEL = `discover:${CHAIN}:updated`;
const DELTA_UPDATE_CHANNEL = `discover:${CHAIN}:updated`;

// === SHARED HELPERS ===
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
  return results.filter(r => r !== undefined) as R[];
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

// Helper to safely compare numeric values
function numericValuesChanged(newVal: any, oldVal: any, threshold = 0.0001): boolean {
  const newNum = Number(newVal ?? 0);
  const oldNum = Number(oldVal ?? 0);
  
  // If both are zero or very small, consider them unchanged
  if (Math.abs(newNum) < threshold && Math.abs(oldNum) < threshold) {
    return false;
  }
  
  // Calculate relative difference
  const diff = Math.abs(newNum - oldNum);
  const avg = (Math.abs(newNum) + Math.abs(oldNum)) / 2;
  
  // Consider changed if absolute difference > threshold OR relative difference > 0.01%
  return diff > threshold || (avg > 0 && diff / avg > 0.0001);
}

// === DELTA WORKER STATE ===
let currentTop20: string[] = [];

/**
 * Listens for ranking changes from the main ranker.worker.
 */
async function listenForRankingChanges() {
  console.log(`DeltaWorker: Subscribing to ${RANKING_CHANGED_CHANNEL}`);
  const subClient = client.duplicate();
  
  await subClient.subscribe(RANKING_CHANGED_CHANNEL, (err, count) => {
    if (err) {
      console.error("DeltaWorker: Failed to subscribe:", err.message);
      return;
    }
    console.log(`DeltaWorker: Subscribed! (count: ${count})`);
  });

  subClient.on("message", (channel, message) => {
    if (channel === RANKING_CHANGED_CHANNEL) {
      try {
        const { top } = JSON.parse(message);
        if (Array.isArray(top)) {
          console.log(`DeltaWorker: Received new top 20 list (${top.length} tokens).`);
          currentTop20 = top;
        }
      } catch (e) {
        console.warn("DeltaWorker: Failed to parse ranking update message", e);
      }
    }
  });
}

/**
 * The main polling loop.
 */
async function runDeltaCheckLoop() {
  console.log(`DeltaWorker: Starting delta check loop (interval: ${DELTA_INTERVAL_MS}ms)`);
  
  setInterval(async () => {
    if (currentTop20.length === 0) {
      return;
    }

    const tokensToPoll = [...currentTop20];
    console.log(`DeltaWorker: Polling ${tokensToPoll.length} tokens for deltas...`);

    // 1. Get NEW LIVE data from the aggregator
    const newSnapshots = await runWithConcurrency<string, AggregatedResult | null>(
      tokensToPoll, 
      CONCURRENCY, 
      fetchSnapshotSafe
    );
    const newSnapMap = new Map(newSnapshots.map(s => [s?.token_address, s]));

    // 2. Get OLD CACHED data from Redis
    const pipeOld = client.multi();
    for (const addr of tokensToPoll) {
      pipeOld.hgetall(`${TOKEN_HASH_PREFIX}${addr}`);
    }
    const oldDataResults = (await pipeOld.exec()) ?? [];

    // 3. Compare, build update pipeline, and publish deltas
    const updatePipe = client.multi();
    const now = Date.now();
    let deltasFound = 0;
    let tokensProcessed = 0;

    for (let i = 0; i < tokensToPoll.length; i++) {
      const addr = tokensToPoll[i];
      const newSnap = newSnapMap.get(addr);
      const oldDataRes = oldDataResults[i];
      const oldData = (Array.isArray(oldDataRes) ? oldDataRes[1] : oldDataRes?.[1]) as Record<string, string> | null;

      if (!newSnap) {
        console.warn(`DeltaWorker: No new snapshot for ${addr}`);
        continue; 
      }

      tokensProcessed++;

      // If no old data exists, this is a new token - publish everything
      if (!oldData || Object.keys(oldData).length === 0) {
        console.log(`DeltaWorker: New token detected (no cache): ${addr}`);
        
        const fullPayload: Record<string, any> = {
          token_address: addr,
          price_sol: String(newSnap.price_sol ?? 0),
          aggregated_price_usd: String(newSnap.aggregated_price_usd ?? 0),
          liquidity_sol: String(newSnap.liquidity_sol ?? 0),
          volume_sol: String(newSnap.volume_sol ?? 0),
          transaction_count: String(newSnap.transaction_count ?? 0),
          price_1hr_change: String(newSnap.price_1hr_change ?? 0),
          price_24hr_change: String(newSnap.price_24hr_change ?? 0),
          last_updated_ts: String(now)
        };

        // Update cache
        updatePipe.hset(`${TOKEN_HASH_PREFIX}${addr}`, fullPayload);
        
        // Publish delta
        updatePipe.publish(DELTA_UPDATE_CHANNEL, JSON.stringify(fullPayload));
        deltasFound++;
        continue;
      }

      // Compare fields
      const fieldsToCompare: Array<keyof AggregatedResult> = [
        "price_sol",
        "aggregated_price_usd",
        "liquidity_sol",
        "volume_sol",
        "transaction_count",
        "price_1hr_change",
        "price_24hr_change"
      ];
      
      const deltaPayload: Record<string, any> = { token_address: addr };
      const hashUpdates: Record<string, string> = {};
      let hasChanged = false;

      for (const field of fieldsToCompare) {
        const newVal = newSnap[field as keyof AggregatedResult];
        const oldVal = oldData[field as string];

        // Use numeric comparison for better accuracy
        if (numericValuesChanged(newVal, oldVal)) {
          hasChanged = true;
          const newValStr = String(newVal ?? 0);
          deltaPayload[field] = newValStr;
          hashUpdates[field] = newValStr;
        }
      }

      if (hasChanged) {
        deltasFound++;
        // Update the cache
        hashUpdates["last_updated_ts"] = String(now);
        updatePipe.hset(`${TOKEN_HASH_PREFIX}${addr}`, hashUpdates);
        
        // Publish the delta
        updatePipe.publish(DELTA_UPDATE_CHANNEL, JSON.stringify(deltaPayload));
      }
    }

    console.log(`DeltaWorker: Processed ${tokensProcessed} tokens, found ${deltasFound} deltas`);

    if (deltasFound > 0) {
      console.log(`DeltaWorker: Executing pipeline with ${deltasFound} deltas...`);
      try {
        await updatePipe.exec();
        console.log(`DeltaWorker: Successfully published ${deltasFound} delta updates`);
      } catch (e) {
        console.error("DeltaWorker: Failed to execute update pipeline", e);
      }
    } else {
      console.log("DeltaWorker: No deltas to publish");
    }

  }, DELTA_INTERVAL_MS);
}

/**
 * Main entry point for the delta worker.
 */
export async function startDeltaWorker(): Promise<void> {
  console.log("Starting delta.worker");
  console.log(" DELTA_INTERVAL_MS=", DELTA_INTERVAL_MS);
  console.log(" RANKING_CHANGED_CHANNEL=", RANKING_CHANGED_CHANNEL);
  console.log(" DELTA_UPDATE_CHANNEL=", DELTA_UPDATE_CHANNEL);

  try {
    await listenForRankingChanges();
    await runDeltaCheckLoop();
  } catch (err) {
    console.error("Delta worker fatal:", err);
    process.exit(1);
  }
}

// Direct execution
const __filename = fileURLToPath(import.meta.url);
const isMain = resolve(process.argv[1] || "") === resolve(__filename);

if (isMain) {
  startDeltaWorker();
}
