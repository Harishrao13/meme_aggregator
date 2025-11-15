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
      await new Promise((resolve) => setTimeout(resolve, delay * Math.pow(2, attempt)));
      attempt += 1;
    }
  }
}

export class AggregatorService {
  /**
   * Try several sources for SOL <-> USD conversion.
   * Returns NaN if unable to determine SOL price.
   */
  static async getSolUsd(
    _jupiterData: JupiterData | null,
    geckoData: GeckoData | null,
    pool: DexPair | null
  ): Promise<number> {
    // 1) Jupiter canonical SOL price
    try {
      const solData = await backoff(() => JupiterService.getJupiterData("SOL") as Promise<unknown>);
      if (Array.isArray(solData) && (solData[0] as any)?.usdPrice) {
        const v = Number((solData[0] as any).usdPrice);
        if (Number.isFinite(v) && v > 0) return v;
      }
      if ((solData as any)?.usdPrice) {
        const v = Number((solData as any).usdPrice);
        if (Number.isFinite(v) && v > 0) return v;
      }
    } catch {
      // ignore and try next source
    }

    // 2) Gecko
    try {
      if (geckoData?.attributes?.symbol && geckoData.attributes.symbol.toUpperCase() === "SOL" && geckoData.attributes.price_usd) {
        const v = Number(geckoData.attributes.price_usd);
        if (Number.isFinite(v) && v > 0) return v;
      }
    } catch {
      // continue
    }

    // 3) Derive from pool if pool exposes priceUsd & priceNative (priceNative is SOL price of quote token)
    try {
      if (pool && (pool as any).priceUsd && (pool as any).priceNative) {
        const priceUsd = Number((pool as any).priceUsd);
        const priceNative = Number((pool as any).priceNative);
        if (Number.isFinite(priceUsd) && Number.isFinite(priceNative) && priceNative > 0) {
          const solUsd = priceUsd / priceNative;
          if (Number.isFinite(solUsd) && solUsd > 0) return solUsd;
        }
      }
    } catch {
      // continue
    }

    // 4) Last resort: attempt to fetch Gecko directly for SOL (best-effort)
    try {
      const g = await backoff(() => GeckoService.getGeckoData("solana", "sol") as Promise<unknown>);
      const gData = (g as any)?.data ?? g;
      if (gData?.attributes?.symbol && gData.attributes.symbol.toUpperCase() === "SOL" && gData.attributes.price_usd) {
        const v = Number(gData.attributes.price_usd);
        if (Number.isFinite(v) && v > 0) return v;
      }
    } catch {
      // continue
    }

    // If all else fails, return NaN to signal missing SOL price (caller will handle)
    return NaN;
  }

  static async liquidityWeightedAggregate(
    tokenAddress: string | null,
    symbol?: string | null
  ): Promise<AggregatedResult> {
    const normalizedAddress: string | null = tokenAddress ? tokenAddress.toLowerCase() : null;
    const normalizedSymbol: string | null = symbol ? symbol.toUpperCase() : null;

    // fetch sources in parallel with retries
    const [dexRes, jupRes, geckoRes] = await Promise.allSettled([
      backoff(() => DexService.getDexData(normalizedAddress as string)).catch(() => null),
      backoff(() => JupiterService.getJupiterData(normalizedSymbol || "")).catch(() => null),
      backoff(() => GeckoService.getGeckoData("solana", normalizedAddress as string)).catch(() => null)
    ]);

    const dexData = dexRes.status === "fulfilled" ? dexRes.value : null;
    const jupiterRaw = jupRes.status === "fulfilled" ? jupRes.value : null;
    const JupiterData: JupiterData | null = jupiterRaw
      ? Array.isArray(jupiterRaw)
        ? (jupiterRaw[0] as JupiterData)
        : (jupiterRaw as JupiterData)
      : null;

    const geckoWrapped = geckoRes.status === "fulfilled" ? geckoRes.value : null;
    const geckoData: GeckoData | null = geckoWrapped ? (geckoWrapped.data ?? geckoWrapped) : null;

    // Address Match
    const pairs: DexPair[] = Array.isArray(dexData?.pairs) ? dexData.pairs : [];
    const matchingPools: DexPair[] = pairs.filter((p) => ((p?.baseToken?.address ?? "") as string).toLowerCase() === (normalizedAddress ?? ""));

    const jupiterSymbolMatch: boolean = !!(JupiterData?.symbol && normalizedSymbol && JupiterData.symbol.toUpperCase() === normalizedSymbol);
    const geckoSymbolMatch: boolean = !!(geckoData?.attributes?.symbol && normalizedSymbol && geckoData.attributes.symbol.toUpperCase() === normalizedSymbol);
    const anyExactSymbolMatch = normalizedSymbol ? (jupiterSymbolMatch || geckoSymbolMatch) : false;

    // If tokenAddress given but no matching pools AND no exact symbol match -> NOT FOUND
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

    // --- 2) Aggregation from matching pools ---
    let totalLiquidityUsd = 0;
    let totalVolumeUsd = 0;
    let weightedPriceSum = 0;
    let weightedHourChangeSum = 0;
    let transactionCount = 0;

    for (const pool of matchingPools) {
      const liquidityUsd = Number((pool.liquidity as any)?.usd ?? 0);
      const volume24hUsd = Number((pool.volume as any)?.h24 ?? 0);
      // priceUsd might be string; prefer pool.priceUsd, fall back to priceNative*solUsd later if needed
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

    // Get SOL USD price (for conversions)
    const solUsd = await this.getSolUsd(JupiterData, geckoData, bestPool);

    // Compute aggregatedPriceUsd:
    //  - If we have liquidity across pools, use liquidity-weighted average.
    //  - Else, attempt to use token-specific data from Jupiter or Gecko (only if symbol matches).
    //  - Else, if bestPool exposes a priceUsd, use it.
    //  - Else, fallback to 0 (unknown).
    let aggregatedPriceUsd: number = 0;

    if (totalLiquidityUsd > 0) {
      aggregatedPriceUsd = weightedPriceSum / totalLiquidityUsd;
    } else {
      // Try Jupiter token price only if symbol matches (prevent using SOL price)
      let tokenPriceFromJupiter: number | null = null;
      if (JupiterData && normalizedSymbol && JupiterData.symbol && JupiterData.symbol.toUpperCase() === normalizedSymbol && JupiterData.usdPrice) {
        const v = Number(JupiterData.usdPrice);
        if (Number.isFinite(v) && v > 0) tokenPriceFromJupiter = v;
      }

      // Try Gecko token price only if symbol matches
      let tokenPriceFromGecko: number | null = null;
      if (geckoData?.attributes?.symbol && normalizedSymbol && geckoData.attributes.symbol.toUpperCase() === normalizedSymbol && geckoData.attributes.price_usd) {
        const v = Number(geckoData.attributes.price_usd);
        if (Number.isFinite(v) && v > 0) tokenPriceFromGecko = v;
      }

      // Try bestPool.priceUsd (some sources include token price there)
      let tokenPriceFromPool: number | null = null;
      try {
        if (bestPool) {
          const p = Number((bestPool as any).priceUsd ?? NaN);
          if (Number.isFinite(p) && p > 0) tokenPriceFromPool = p;
        }
      } catch {
        tokenPriceFromPool = null;
      }

      aggregatedPriceUsd =
        tokenPriceFromJupiter ??
        tokenPriceFromGecko ??
        tokenPriceFromPool ??
        0;
    }

    // Compute aggregated price in SOL (token price in SOL)
    // aggregated_price_sol = aggregated_price_usd / solUsd
    let aggregatedPriceSol: number = 0;
    if (aggregatedPriceUsd > 0 && Number.isFinite(solUsd) && solUsd > 0) {
      aggregatedPriceSol = aggregatedPriceUsd / solUsd;
    } else {
      // if no SOL price available, and pool provides priceNative or priceSol, try to derive
      if (bestPool) {
        const poolPriceSol = Number((bestPool as any).priceSol ?? (bestPool as any).priceNative ?? NaN);
        if (Number.isFinite(poolPriceSol) && poolPriceSol > 0) {
          // If aggregatedPriceUsd is known, prefer divider; otherwise use pool price as best-effort
          aggregatedPriceSol = aggregatedPriceUsd > 0 ? aggregatedPriceUsd / solUsd : poolPriceSol;
        }
      }
      // if still zero, leave as 0
    }

    // liquidity_sol / volume_sol: convert total USD aggregates to SOL if solUsd available
    const liquiditySol = Number.isFinite(solUsd) && solUsd > 0 ? totalLiquidityUsd / solUsd : totalLiquidityUsd;
    const volumeSol = Number.isFinite(solUsd) && solUsd > 0 ? totalVolumeUsd / solUsd : totalVolumeUsd;

    // token_name and ticker resolution
    const tokenName =
      bestPool?.baseToken?.name ?? JupiterData?.name ?? geckoData?.attributes?.name ?? "";
    const tokenTicker =
      bestPool?.baseToken?.symbol ?? JupiterData?.symbol ?? geckoData?.attributes?.symbol ?? (symbol ?? "");

    return {
      token_address: normalizedAddress,
      token_name: tokenName,
      token_ticker: tokenTicker,
      price_sol: aggregatedPriceSol ?? 0,
      liquidity_sol: liquiditySol ?? 0,
      volume_sol: volumeSol ?? 0,
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
