import { createClient } from "redis";

const r = createClient();
await r.connect();

await r.publish(
  "discover:solana:updated",
  JSON.stringify({ top: ["TOKEN1", "TOKEN2"] })
);

console.log("published!");
