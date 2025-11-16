export interface RedisTokenHash {
  token_address: string;                 
  token_name: string;
  token_ticker: string;

  price_sol: string;                     
  aggregated_price_usd: string;

  liquidity_sol: string;
  volume_sol: string;
  transaction_count: string;

  price_1hr_change: string;
  price_24hr_change: string;
  price_7d_change: string;

  last_updated_ts: string;

  score_raw: string;
  score_scaled: string;

  last_top_ts?: string;

  aging_cycle_count?: string;
}
