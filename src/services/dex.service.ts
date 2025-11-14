import axios from "axios";

export class DexService {
  static async getDexData(tokenAddress: string) {
    try {
      const url = `${process.env.DEX_SCREENER_BASE_URL}/tokens/${tokenAddress}`;
      const { data } = await axios.get(url);
      return data;
    }  catch (err) {
        const error = err as Error;
        console.error("DexScreener Error:", error.message);
        throw new Error("Failed to fetch DexScreener data");
        }

  }

  // 🟡 2) Jupiter API (Commented until we parse format)
  /*
  static async getJupiterData(query: string) {
    try {
      const url = `${process.env.JUPITER_BASE_URL}/tokens/v2/search?query=${query}`;
      const { data } = await axios.get(url);
      return data;
    } catch (err) {
      console.error("Jupiter Error:", err.message);
      throw new Error("Failed to fetch Jupiter data");
    }
  }
  */

  // 🔴 3) GeckoTerminal (Commented until ready)
  /*
  static async getGeckoTerminalData(network: string, address: string) {
    try {
      const url = `${process.env.GECKO_TERMINAL_BASE_URL}/networks/${network}/tokens/${address}`;
      const { data } = await axios.get(url, {
        headers: {
          "x-api-key": process.env.GECKO_TERMINAL_API_KEY!
        }
      });
      return data;
    } catch (err) {
      console.error("GeckoTerminal Error:", err.message);
      throw new Error("Failed to fetch GeckoTerminal data");
    }
  }
  */

  // 🧠 Later we aggregate here:
  /*
  static async aggregateTokenData(params) {
      const [dex, jup, gecko] = await Promise.all([
         this.getDexScreenerData(params.address),
         this.getJupiterData(params.symbol),
         this.getGeckoTerminalData(params.network, params.address)
      ]);
      
      // return merged format
  }
  */
}
