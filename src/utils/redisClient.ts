import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const redisHost = process.env.REDIS_HOST ?? "127.0.0.1";
const redisPort = Number(process.env.REDIS_PORT ?? 6379);
const redisPassword =
  process.env.REDIS_PASSWORD && process.env.REDIS_PASSWORD.trim() !== ""
    ? process.env.REDIS_PASSWORD
    : undefined;

const redisClient = new (Redis as any)({
  host: redisHost,
  port: redisPort,
  password: redisPassword,
  maxRetriesPerRequest: 3
})

redisClient.on("connect", () => {
  console.log(`Connected to Redis at ${redisHost}:${redisPort}`);
});

redisClient.on("error", (err: Error) => {
  console.error("Redis Connection Error:", err);
});

export default redisClient;
