import axios from "axios";
import { DexService } from "./dex.service.js";
import { JupiterService } from "./jupiter.service.js";
import { GeckoService } from "./gecko.service.js";

import { DexPair } from "../models/DexPair.js"

async function backoff(fn: () => Promise<any>, retries = 3, delay = 300) {
  let attempt = 0;
  while (attempt <= retries) {
    try {
      return await fn();
    } catch (e) {
      if (attempt === retries) throw e;
      await new Promise(r => setTimeout(r, delay * Math.pow(2, attempt)));
      attempt++;
    }
  }
}

export class AggregatorService {
  static async getSolUsd(jup: any, gecko: any, pool: any) {
    try {
      const sol = await backoff(() => JupiterService.getJupiterData("SOL"));
      if (sol?.[0]?.usdPrice) return Number(sol[0].usdPrice);
    } catch {}

    try {
      if (gecko?.attributes?.symbol === "SOL") {
        return Number(gecko.attributes.price_usd);
      }
    } catch {}

    try {
      if (pool?.priceUsd && pool?.priceNative) {
        return Number(pool.priceUsd) / Number(pool.priceNative);
      }
    } catch {}

    return 1;
  }

  static async liquidityWeightedAggregate(tokenAddress: string, symbol?: string) {
    const [dexRes, jupRes, geckoRes] = await Promise.allSettled([
      backoff(() => DexService.getDexData(tokenAddress)),
      backoff(() => JupiterService.getJupiterData(symbol || "")),
      backoff(() => GeckoService.getGeckoData("solana", tokenAddress))
    ]);

    const dex = dexRes.status === "fulfilled" ? dexRes.value : null;
    const jup = jupRes.status === "fulfilled" ? (Array.isArray(jupRes.value) ? jupRes.value[0] : jupRes.value) : null;
    const gecko = geckoRes.status === "fulfilled" ? geckoRes.value?.data ?? null : null;

    const pools: DexPair[] = Array.isArray(dex?.pairs) ? dex.pairs : [];
    const validPools = pools.filter(p => p?.baseToken?.address === tokenAddress);
    

    let totalLiq = 0;
    let totalVol = 0;
    let weightedPrice = 0;
    let weightedHr = 0;
    let txCount = 0;

    for (const p of validPools) {
      const liq = Number(p?.liquidity?.usd || 0);
      const vol = Number(p?.volume?.h24 || 0);
      const price = Number(p?.priceUsd || 0);
      const change = Number(p?.priceChange?.h1 || 0);
      const buys = Number(p?.txns?.h24?.buys || 0);
      const sells = Number(p?.txns?.h24?.sells || 0);

      totalLiq += liq;
      totalVol += vol;
      weightedPrice += price * liq;
      weightedHr += change * liq;
      txCount += buys + sells;
    }

    const bestPool = validPools.reduce((t, p) =>
      Number(p?.liquidity?.usd || 0) > Number(t?.liquidity?.usd || 0) ? p : t,
      validPools[0] || null
    );

    const solUsd = await this.getSolUsd(jup, gecko, bestPool);
    const aggregatedPriceUsd = totalLiq > 0 ? weightedPrice / totalLiq : Number(jup?.usdPrice || gecko?.attributes?.price_usd || 0);
    const aggregatedPriceSol = solUsd ? aggregatedPriceUsd / solUsd : aggregatedPriceUsd;

    const liquiditySol = solUsd ? totalLiq / solUsd : totalLiq;
    const volumeSol = solUsd ? totalVol / solUsd : totalVol;
    const hrChange = totalLiq > 0 ? weightedHr / totalLiq : Number(jup?.stats1h?.priceChange || 0);

    const bestUsd = jup?.usdPrice ? Number(jup.usdPrice) : Number(bestPool?.priceUsd || 0);
    const bestSol = solUsd ? bestUsd / solUsd : bestUsd;

    const protocol = bestPool
      ? `${String(bestPool.dexId).toUpperCase()} ${((bestPool.labels || []).join(" "))}`.trim()
      : jup ? "JUPITER" : "UNKNOWN";

    return {
      token_address: bestPool?.baseToken?.address || jup?.id || gecko?.attributes?.address || tokenAddress,
      token_name: bestPool?.baseToken?.name || jup?.name || gecko?.attributes?.name || "",
      token_ticker: bestPool?.baseToken?.symbol || jup?.symbol || gecko?.attributes?.symbol || symbol || "",
      price_sol: aggregatedPriceSol || 0,
      market_cap_sol: gecko?.attributes?.market_cap_usd ? Number(gecko.attributes.market_cap_usd) / solUsd : 0,
      volume_sol: volumeSol || 0,
      liquidity_sol: liquiditySol || 0,
      transaction_count: txCount || 0,
      price_1hr_change: hrChange || 0,
      protocol,
      aggregated_price_usd: aggregatedPriceUsd || 0,
      aggregated_price_sol: aggregatedPriceSol || 0,
      best_price_usd: bestUsd || 0,
      best_price_sol: bestSol || 0,
      best_pool: bestPool
        ? {
            dexId: bestPool.dexId,
            pairAddress: bestPool.pairAddress,
            priceUsd: bestPool.priceUsd,
            liquidityUsd: bestPool.liquidity?.usd
          }
        : null
    };
  }
}
