import express, { Application } from "express";
import dotenv from "dotenv";
import cors from "cors";

import priceRoutes from "./routes/price.route.js";
import discoverRoutes from "./routes/discover.route.js";

dotenv.config();

const app: Application = express();

const allowed = (process.env.CORS_ALLOWED_ORIGINS || "http://localhost:3000")
  .split(",")
  .map(s => s.trim());

app.use(cors({
  origin: (origin, callback) => {
    // allow requests with no origin (like mobile apps, curl, server-side)
    if (!origin) return callback(null, true);
    if (allowed.includes(origin)) return callback(null, true);
    return callback(new Error("CORS not allowed"));
  },
  credentials: true, // if you use cookies/auth
}));

app.use(express.json());

app.use("/price", priceRoutes);
app.use("/discover", discoverRoutes);

// Sample hello API
app.get("/hello", (req, res) => {
  res.send("Hello from Express!");
});

export default app;
