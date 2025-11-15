import { Router } from "express";
import { DiscoverController } from "../controllers/discover.controller.js";
const router = Router();

router.get("/", DiscoverController.getDiscover);

export default router;
