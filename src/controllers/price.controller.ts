import { Request, Response } from "express";
import { AggregatorService } from "../services/aggregator.service.js";
import redisClient from "../utils/redisClient.js";

const CACHE_TTL_SECONDS = Number(process.env.PRICE_CACHE_TTL ?? 30);

export class PriceController {
  static async testAPIs(req: Request, res: Response) {
    try {
      const tokenAddress =
        (req.query.tokenAddress as string) ||
        (req.body?.tokenAddress as string);

      const symbol =
        (req.query.symbol as string) || (req.body?.symbol as string);

      if (!tokenAddress) {
        return res.status(400).json({ error: "tokenAddress is required" });
      }

      const cacheKey = `price:${tokenAddress.toLowerCase()}:${(symbol || "").toUpperCase()}`;

      const cached = await redisClient.get(cacheKey);
      if (cached) {
        const parsed = JSON.parse(cached);
        return res.json({ ...parsed, _cache: "HIT" });
      }

      const aggregated = await AggregatorService.liquidityWeightedAggregate(
        tokenAddress,
        symbol
      );

      await redisClient.set(cacheKey, JSON.stringify(aggregated), "EX", CACHE_TTL_SECONDS);

      return res.json({ ...aggregated, _cache: "MISS" });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error("PriceController Error:", message);
      return res.status(500).json({ error: "Unexpected server error", message });
    }
  }
}
