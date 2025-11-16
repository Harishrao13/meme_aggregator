import type { Request, Response } from "express";
import type { Redis } from "ioredis";
import redis from "../utils/redisClient.js";
import { runRankingOnce, SCORE_ZSET_KEY, TOKEN_HASH_PREFIX } from "../workers/ranking.worker.js";

const client = redis as unknown as Redis;

function toNum(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(String(v).trim());
  return Number.isFinite(n) ? n : null;
}

export class DiscoverController {
  static async getDiscover(req: Request, res: Response) {
    try {
      const limit = Math.max(1, Math.min(200, Number(req.query.limit ?? 40)));
      const refresh = req.query.refresh === "1" || req.query.refresh === "true";

      if (refresh) {
        await runRankingOnce(limit);
      }

      // fetch top tokens from zset (base ordering is by score desc)
      const members: string[] = await client.zrevrange(SCORE_ZSET_KEY, 0, limit - 1);

      // fetch hashes
      const pipeline = client.multi();
      for (const m of members) pipeline.hgetall(`${TOKEN_HASH_PREFIX}${m}`);
      const replies = await pipeline.exec();

      const results: any[] = [];
      if (Array.isArray(replies)) {
        for (let i = 0; i < replies.length; i++) {
          const reply = replies[i];
          const result = Array.isArray(reply) && reply.length === 2 ? reply[1] : reply;
          const parsed = (result && typeof result === "object") ? result as Record<string,string> : {};

          // normalize candidate metric values (try several field names)
          const volumeUsd = toNum(parsed.volume_usd ?? parsed.volumeUsd ?? parsed.volume_h24 ?? parsed.volume24h ?? parsed.volume_sol);
          const aggregatedPriceUsd = toNum(parsed.aggregated_price_usd ?? parsed.price_usd ?? parsed.priceUsd ?? parsed.aggregatedPriceUsd);
          const priceSol = toNum(parsed.price_sol ?? parsed.priceSol);
          const marketCapUsd = toNum(parsed.market_cap_usd ?? parsed.marketCapUsd ?? parsed.market_cap ?? parsed.market_cap_usd);
          const priceChange24 = toNum(parsed.price_change_24h ?? parsed.price_change_percent_24h ?? parsed.change_24h ?? parsed.price_change);

          const scoreScaled = toNum(parsed.score_scaled ?? parsed.score_scaled);
          const scoreRaw = toNum(parsed.score_raw ?? parsed.score_raw);

          results.push({
            rank: i + 1,
            token_address: parsed.token_address ?? members[i],
            token_name: parsed.token_name ?? null,
            token_ticker: parsed.token_ticker ?? null,
            price_sol: priceSol,
            aggregated_price_usd: aggregatedPriceUsd,
            liquidity_sol: toNum(parsed.liquidity_sol ?? parsed.liquidity),
            volume_usd: volumeUsd,
            transaction_count: toNum(parsed.transaction_count ?? parsed.tx_count),
            market_cap_usd: marketCapUsd,
            price_change_24h: priceChange24,
            score_scaled: scoreScaled,
            score_raw: scoreRaw,
            last_updated_ts: toNum(parsed.last_updated_ts ?? parsed.last_discovered_ts ?? parsed.updated_ts ?? parsed.ts) ?? null,
          });
        }
      }

      const finalResults = results.slice(0, limit);

      const meta = {
        requested_limit: limit,
        returned: finalResults.length,
      };

      return res.json({ ok: true, meta, results: finalResults });
    } catch (err: any) {
      console.error("DiscoverController.getDiscover error:", err);
      return res.status(500).json({ error: "internal_error", message: err?.message ?? String(err) });
    }
  }
}
