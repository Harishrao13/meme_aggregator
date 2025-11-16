### Real-Time Meme Coin Aggregation Service
A scalable backend replicating the real-time token discovery flow of axiom.trade

Live Demo: https://youtu.be/OHXczKkvuK8

This project implements a real-time data aggregation backend for Solana meme coins, combining multiple DEX APIs, Redis-based caching, workers for ranking/discovery/deltas, and a WebSocket system for live token updates.

It mirrors the behavior shown on axiom.trade/discover:

Initial load via REST API

Subsequent updates via WebSockets (no new HTTP calls)

Fresh, merged, deduped token data from multiple DEX sources

### Features

✅ Real-Time Aggregation

Aggregates live token data from:

DexScreener API

Jupiter API

GeckoTerminal API

Intelligent merging of duplicate tokens

Rate limiting handled via exponential backoff

⚡ Redis Caching

Configurable TTL (default: 30 seconds)

Avoids redundant upstream API calls

### Redis stores:

Token set

Ranked token list

Token hashes

Delta comparison reference

Fully optimized for high-frequency reads & writes

### Workers & Scheduling

Modular worker architecture for scalability:

 Worker	Responsibility:
```
discovery.worker:	Discovers new tokens across APIs, populates Redis SET
ranking.worker:	Ranks tokens using custom scoring + aging technique
delta.worker:	Fetches top 20 tokens, compares with Redis hashes, pushes WebSocket updates
scheduler.cron:	Schedules workers using node-cron / modular scheduling
```

