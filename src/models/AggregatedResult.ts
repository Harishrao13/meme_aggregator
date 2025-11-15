export interface AggregatedResult {
  token_address: string | null;
  token_name: string;
  token_ticker: string;
  price_sol: number;
  liquidity_sol: number;
  volume_sol: number;
  transaction_count: number;
  aggregated_price_usd: number;
  aggregated_price_sol: number;
  best_pool: null | {
    dexId?: string;
    pairAddress?: string;
    priceUsd?: number | string;
    liquidityUsd?: number | string;
  };
  protocol: string;
  error?: string;
  symbol?: string | null;
}