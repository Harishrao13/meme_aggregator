import cron from "node-cron";
import { runRankingOnce } from "./ranking.worker.js";
import { runDiscoveryLoop } from "./discovery.worker.js";

const DISCOVERY_CRON = process.env.DISCOVERY_CRON ?? "*/1 * * * *"; // every minute
const RANKER_CRON = process.env.RANKER_CRON ?? "*/1 * * * *";     // every minute

console.log("scheduler.cron: starting. DISCOVERY_CRON=", DISCOVERY_CRON, "RANKER_CRON=", RANKER_CRON);

// discovery: run single iteration on schedule
cron.schedule(DISCOVERY_CRON, async () => {
  const ts = new Date().toISOString();
  console.log(`[scheduler.cron] discovery job started at ${ts}`);
  try {
    await runDiscoveryLoop({ singleIteration: true } as any);
    console.log(`[scheduler.cron] discovery job finished at ${new Date().toISOString()}`);
  } catch (err) {
    console.error(`[scheduler.cron] discovery job failed:`, err);
  }
});

// ranker: run single ranking pass on schedule
cron.schedule(RANKER_CRON, async () => {
  const ts = new Date().toISOString();
  console.log(`[scheduler.cron] ranker job started at ${ts}`);
  try {
    await runRankingOnce();
    console.log(`[scheduler.cron] ranker job finished at ${new Date().toISOString()}`);
  } catch (err) {
    console.error(`[scheduler.cron] ranker job failed:`, err);
  }
});

// keep process alive if run directly
console.log("scheduler.cron: jobs scheduled");
