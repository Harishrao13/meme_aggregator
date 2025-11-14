import axios from "axios";

export class JupiterService {
  static async getJupiterData(query: string) {
    try {
      const url = `${process.env.JUPITER_BASE_URL}/tokens/v2/search?query=${query}`;
      const { data } = await axios.get(url);
      return data;
    } catch (err) {
      throw new Error("Jupiter API failed");
    }
  }
}
