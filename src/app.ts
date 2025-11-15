import express, { Application } from "express";
import dotenv from "dotenv";

import priceRoutes from "./routes/price.route.js"
import discoverRoutes from "./routes/discover.route.js"

dotenv.config();

const app: Application = express();

app.use(express.json());

app.use("/price", priceRoutes)
app.use("/discover", discoverRoutes)

// Sample hello API
app.get("/hello", (req, res) => {
  res.send("Hello from Express!");
});

export default app;