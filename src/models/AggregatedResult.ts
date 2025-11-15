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
  scoreComponents?: ScoreComponents
  price_1hr_change?: Number
  price_24hr_change?: Number
  price_7d_change?: Number

}

export interface ScoreComponents {
  liquidity_log: number;
  volume_log: number;
  txcount_log: number;
  price_change_abs: number;
}
