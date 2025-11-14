import { Request, Response } from "express";
import { AggregatorService } from "../services/aggregator.service.js";

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

      // Call the aggregator (handles fetching Dex/Jupiter/Gecko and merging)
      const aggregated = await AggregatorService.liquidityWeightedAggregate(
        tokenAddress,
        symbol
      );

      // Optionally: drop raw debug blobs before returning in production
      // delete aggregated._raw;

      return res.json(aggregated);
    } catch (err) {
      // err is unknown by default in TS catch; narrow it safely
      const message = err instanceof Error ? err.message : String(err);
      console.error("PriceController Error:", message);

      return res.status(500).json({
        error: "Unexpected server error",
        message
      });
    }
  }
}
