import { Router } from "express";
import { PriceController } from "../controllers/price.controller.js";

const router = Router();

// GET /price/test?tokenAddress=xxxxx
router.get("/test", PriceController.testAPIs);

export default router;
