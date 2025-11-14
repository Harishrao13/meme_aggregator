export interface DexPair {
  baseToken?: {
    address?: string;
    name?: string;
    symbol?: string;
  };
  priceUsd?: string | number;
  priceNative?: string | number;
  liquidity?: { usd?: number };
  volume?: { h24?: number };
  txns?: { h24?: { buys?: number; sells?: number } };
  priceChange?: { h1?: number };
  dexId?: string;
  pairAddress?: string;
  labels?: string[];
}
