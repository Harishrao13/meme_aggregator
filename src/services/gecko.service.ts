import axios from "axios"

export class GeckoService {
  static async getGeckoData(network: string, token: string) {
    try {
      const url = `${process.env.GECKO_TERMINAL_BASE_URL}/networks/${network}/tokens/${token}`;
      const { data } = await axios.get(url, {
        headers: { "x-api-key": process.env.GECKO_TERMINAL_API_KEY }
      });
      return data;
    } catch (err) {
      throw new Error("GeckoTerminal failed");
    }
  }
}
