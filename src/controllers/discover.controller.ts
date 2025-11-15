import type { Request, Response } from "express";
import type { Redis } from "ioredis";
import redis from "../utils/redisClient.js";
import { runRankingOnce, SCORE_ZSET_KEY, TOKEN_HASH_PREFIX } from "../workers/ranking.worker.js";

const client = redis as unknown as Redis;

/**
 * GET /discover
 * - limit: number (default 20)
 * - refresh: boolean (optional) -> run ranking once before returning
 */
export class DiscoverController {
  static async getDiscover(req: Request, res: Response) {
    try {
      const limit = Math.max(1, Math.min(200, Number(req.query.limit ?? 20)));
      const refresh = req.query.refresh === "1" || req.query.refresh === "true";

      if (refresh) {
        await runRankingOnce(limit);
      }

      // fetch top tokens from zset
      const members: string[] = await client.zrevrange(SCORE_ZSET_KEY, 0, limit - 1);
      const pipeline = client.multi();
      for (const m of members) pipeline.hgetall(`${TOKEN_HASH_PREFIX}${m}`);
      const replies = await pipeline.exec();
      const results: any[] = [];
      if (Array.isArray(replies)) {
        for (let i = 0; i < replies.length; i++) {
          const reply = replies[i];
          const result = Array.isArray(reply) && reply.length === 2 ? reply[1] : reply;
          const parsed = (result && typeof result === "object") ? result as Record<string,string> : {};
          results.push({
            rank: i + 1,
            token_address: parsed.token_address ?? members[i],
            token_name: parsed.token_name ?? null,
            token_ticker: parsed.token_ticker ?? null,
            price_sol: parsed.price_sol ? Number(parsed.price_sol) : null,
            aggregated_price_usd: parsed.aggregated_price_usd ? Number(parsed.aggregated_price_usd) : null,
            liquidity_sol: parsed.liquidity_sol ? Number(parsed.liquidity_sol) : null,
            volume_sol: parsed.volume_sol ? Number(parsed.volume_sol) : null,
            transaction_count: parsed.transaction_count ? Number(parsed.transaction_count) : null,
            score_scaled: parsed.score_scaled ? Number(parsed.score_scaled) : null,
            score_raw: parsed.score_raw ? Number(parsed.score_raw) : null,
            last_updated_ts: parsed.last_updated_ts ? Number(parsed.last_updated_ts) : null,
            raw: parsed
          });
        }
      }

      return res.json({ ok: true, limit, count: results.length, results });
    } catch (err: any) {
      console.error("DiscoverController.getDiscover error:", err);
      return res.status(500).json({ error: "internal_error", message: err?.message ?? String(err) });
    }
  }
}
