export function monthlyIncomeStatement(ledger, month) {
  if (!/^\d{4}-\d{2}$/.test(month)) {
    throw new Error("month must use YYYY-MM");
  }

  let income = 0;
  let expense = 0;

  for (const transaction of ledger.transactions) {
    if (!transaction.date.startsWith(month)) continue;

    for (const entry of transaction.entries) {
      const account = ledger.accounts.get(entry.account);

      if (account.type === "income") {
        income += entry.credit - entry.debit;
      }

      if (account.type === "expense") {
        expense += entry.debit - entry.credit;
      }
    }
  }

  return {
    month,
    income,
    expense,
    netIncome: income - expense
  };
}
