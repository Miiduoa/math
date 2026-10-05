import { loadLedger } from "./src/io.js";
import { formatMinor } from "./src/money.js";
import { monthlyIncomeStatement } from "./src/reports.js";

const path = process.argv[2];

if (!path) {
  console.error("Usage: node cli.js <ledger.json>");
  process.exit(2);
}

const ledger = loadLedger(path);
const balances = ledger.balances();
const trial = ledger.trialBalance();

console.log("Account balances");
for (const [account, amount] of Object.entries(balances)) {
  console.log(`${account.padEnd(12)} ${formatMinor(amount)}`);
}

console.log("\nTrial balance");
console.log({
  debit: formatMinor(trial.debit),
  credit: formatMinor(trial.credit),
  balanced: trial.balanced
});

const months = [
  ...new Set(
    ledger.transactions.map((transaction) =>
      transaction.date.slice(0, 7)
    )
  )
].sort();

console.log("\nMonthly income statements");
for (const month of months) {
  const report = monthlyIncomeStatement(ledger, month);
  console.log(month, {
    income: formatMinor(report.income),
    expense: formatMinor(report.expense),
    netIncome: formatMinor(report.netIncome)
  });
}
