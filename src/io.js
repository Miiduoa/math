import fs from "node:fs";
import { Ledger } from "./ledger.js";

export function loadLedger(path) {
  const payload = JSON.parse(fs.readFileSync(path, "utf8"));
  const ledger = new Ledger(payload.accounts ?? []);

  for (const transaction of payload.transactions ?? []) {
    ledger.post(transaction);
  }

  return ledger;
}
