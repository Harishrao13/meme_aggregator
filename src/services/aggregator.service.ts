import { DexPair } from "../models/DexPair.js";
import { DexService } from "./dex.service.js";
import { JupiterService } from "./jupiter.service.js";
import { GeckoService } from "./gecko.service.js";

import { AggregatedResult } from "../models/AggregatedResult.js";
import { JupiterData } from "../models/Jupiter.js";
import { GeckoData } from "../models/GeckoData.js";

async function backoff<T>(fn: () => Promise<T>, retries = 3, delay = 300): Promise<T> {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (err) {
      if (attempt >= retries) throw err;
      // exponential backoff
      await new Promise((resolve) => setTimeout(resolve, delay * Math.pow(2, attempt)));
      attempt += 1;
    }
  }
}

export class AggregatorService {
  /**
   * Try several sources for SOL <-> USD conversion
   */
  static async getSolUsd(
    jupiterData: JupiterData | null,
    geckoData: GeckoData | null,
    pool: DexPair | null
  ): Promise<number> {
    try {
      const solData = await backoff(() => JupiterService.getJupiterData("SOL") as Promise<unknown>);
      // solData may be array or object
      if (Array.isArray(solData) && (solData[0] as any)?.usdPrice) {
        return Number((solData[0] as any).usdPrice);
      }
      if ((solData as any)?.usdPrice) return Number((solData as any).usdPrice);
    } catch {
      // fall through to next source
    }

    try {
      if (geckoData?.attributes?.symbol?.toUpperCase() === "SOL" && geckoData.attributes.price_usd) {
        return Number(geckoData.attributes.price_usd);
      }
    } catch {
      // fall through
    }

    try {
      if (pool?.priceUsd && (pool as any).priceNative) {
        return Number((pool as any).priceUsd) / Number((pool as any).priceNative);
      }
    } catch {
      // fall through
    }

    // fallback default
    return 1;
  }

  /**
   * liquidityWeightedAggregate
   * - tokenAddress is authoritative (address-based lookup)
   * - symbol is optional and only used to help find Jupiter/gecko results, NOT to replace token_address
   * - If both tokenAddress and symbol are provided, tokenAddress has priority. If tokenAddress lookup fails
   *   and symbol does not exactly match on Jupiter/Gecko, return error.
   */
  static async liquidityWeightedAggregate(
    tokenAddress: string | null,
    symbol?: string | null
  ): Promise<AggregatedResult> {
    const normalizedAddress: string | null = tokenAddress ? tokenAddress.toLowerCase() : null;
    const normalizedSymbol: string | null = symbol ? symbol.toUpperCase() : null;

    // fetch sources in parallel with retries
    const [dexRes, jupRes, geckoRes] = await Promise.allSettled([
      backoff(() => DexService.getDexData(normalizedAddress as string)).catch((e) => null),
      backoff(() => JupiterService.getJupiterData(normalizedSymbol || "")).catch((e) => null),
      backoff(() => GeckoService.getGeckoData("solana", normalizedAddress as string)).catch((e) => null)
    ]);

    const dexData = dexRes.status === "fulfilled" ? dexRes.value : null;
    const jupiterRaw = jupRes.status === "fulfilled" ? jupRes.value : null;

    // Jupiter sometimes returns an array or an object
    const JupiterData: JupiterData | null = jupiterRaw
      ? Array.isArray(jupiterRaw)
        ? (jupiterRaw[0] as JupiterData)
        : (jupiterRaw as JupiterData)
      : null;

    const geckoWrapped = geckoRes.status === "fulfilled" ? geckoRes.value : null;
    const geckoData: GeckoData | null = geckoWrapped ? (geckoWrapped.data ?? geckoWrapped) : null;

    // --- 1) STRICT ADDRESS MATCH ---
    const pairs: DexPair[] = Array.isArray(dexData?.pairs) ? dexData.pairs : [];
    const matchingPools: DexPair[] = pairs.filter((p) => ((p?.baseToken?.address ?? "") as string).toLowerCase() === (normalizedAddress ?? ""));

    // Exact symbol matches (used only when address lookup isn't decisive)
    const jupiterSymbolMatch: boolean = !!(JupiterData?.symbol && normalizedSymbol && JupiterData.symbol.toUpperCase() === normalizedSymbol);
    const geckoSymbolMatch: boolean = !!(geckoData?.attributes?.symbol && normalizedSymbol && geckoData.attributes.symbol.toUpperCase() === normalizedSymbol);

    const anyExactSymbolMatch = normalizedSymbol ? (jupiterSymbolMatch || geckoSymbolMatch) : false;

    // --- 2) If tokenAddress given but no matching pools AND no exact symbol match -> NOT FOUND
    // Priority: tokenAddress first. Only if address not found and symbol is an exact match do we continue.
    if (normalizedAddress && matchingPools.length === 0 && !anyExactSymbolMatch) {
      return {
        error: "Token doesn't exist",
        token_address: normalizedAddress,
        symbol: normalizedSymbol ?? null,
        token_name: "",
        token_ticker: normalizedSymbol ?? "",
        price_sol: 0,
        liquidity_sol: 0,
        volume_sol: 0,
        transaction_count: 0,
        aggregated_price_usd: 0,
        aggregated_price_sol: 0,
        best_pool: null,
        protocol: "UNKNOWN"
      };
    }

    // --- 3) Continue aggregation ---
    let totalLiquidityUsd = 0;
    let totalVolumeUsd = 0;
    let weightedPriceSum = 0;
    let weightedHourChangeSum = 0;
    let transactionCount = 0;

    for (const pool of matchingPools) {
      const liquidityUsd = Number((pool.liquidity as any)?.usd ?? 0);
      const volume24hUsd = Number((pool.volume as any)?.h24 ?? 0);
      const priceUsd = Number((pool as any).priceUsd ?? 0);

      totalLiquidityUsd += liquidityUsd;
      totalVolumeUsd += volume24hUsd;
      weightedPriceSum += priceUsd * liquidityUsd;
      weightedHourChangeSum += Number((pool as any).priceChange?.h1 ?? 0) * liquidityUsd;
      transactionCount += Number((pool as any).txns?.h24?.buys ?? 0) + Number((pool as any).txns?.h24?.sells ?? 0);
    }

    const bestPool: DexPair | null = matchingPools.length > 0
      ? matchingPools.reduce((best, curr) => Number((curr as any).liquidity?.usd ?? 0) > Number((best as any).liquidity?.usd ?? 0) ? curr : best, matchingPools[0])
      : null;

    const solUsd = await this.getSolUsd(JupiterData, geckoData, bestPool);

    const aggregatedPriceUsd = totalLiquidityUsd > 0
      ? weightedPriceSum / totalLiquidityUsd
      : Number(JupiterData?.usdPrice ?? geckoData?.attributes?.price_usd ?? 0);

    const aggregatedPriceSol = solUsd ? aggregatedPriceUsd / solUsd : aggregatedPriceUsd;

    return {
      token_address: normalizedAddress,
      token_name: bestPool?.baseToken?.name ?? JupiterData?.name ?? geckoData?.attributes?.name ?? "",
      token_ticker: bestPool?.baseToken?.symbol ?? JupiterData?.symbol ?? geckoData?.attributes?.symbol ?? (symbol ?? ""),
      price_sol: aggregatedPriceSol ?? 0,
      liquidity_sol: solUsd ? totalLiquidityUsd / solUsd : totalLiquidityUsd,
      volume_sol: solUsd ? totalVolumeUsd / solUsd : totalVolumeUsd,
      transaction_count: transactionCount,
      aggregated_price_usd: aggregatedPriceUsd ?? 0,
      aggregated_price_sol: aggregatedPriceSol ?? 0,
      best_pool: bestPool
        ? {
            dexId: (bestPool as any).dexId,
            pairAddress: (bestPool as any).pairAddress,
            priceUsd: (bestPool as any).priceUsd,
            liquidityUsd: (bestPool as any).liquidity?.usd
          }
        : null,
      protocol: bestPool ? (bestPool as any).dexId ?? "UNKNOWN" : "UNKNOWN"
    };
  }
}
