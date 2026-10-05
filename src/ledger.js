const ACCOUNT_TYPES = new Set([
  "asset",
  "liability",
  "income",
  "expense",
  "equity"
]);

function normalSide(type) {
  if (type === "asset" || type === "expense") {
    return "debit";
  }
  return "credit";
}

export class Ledger {
  constructor(accounts = []) {
    this.accounts = new Map();
    this.transactions = [];
    this.transactionIds = new Set();

    for (const account of accounts) {
      this.addAccount(account);
    }
  }

  addAccount(account) {
    if (!account?.id?.trim()) {
      throw new Error("account id is required");
    }

    if (!ACCOUNT_TYPES.has(account.type)) {
      throw new Error(`invalid account type: ${account.type}`);
    }

    if (this.accounts.has(account.id)) {
      throw new Error(`duplicate account id: ${account.id}`);
    }

    this.accounts.set(account.id, {
      id: account.id,
      name: account.name ?? account.id,
      type: account.type
    });
  }

  post(transaction) {
    validateTransaction(transaction, this.accounts);

    if (this.transactionIds.has(transaction.id)) {
      throw new Error(`duplicate transaction id: ${transaction.id}`);
    }

    const frozen = structuredClone(transaction);
    this.transactions.push(frozen);
    this.transactionIds.add(transaction.id);
  }

  balance(accountId) {
    const account = this.accounts.get(accountId);
    if (!account) {
      throw new Error(`unknown account: ${accountId}`);
    }

    let debit = 0;
    let credit = 0;

    for (const transaction of this.transactions) {
      for (const entry of transaction.entries) {
        if (entry.account !== accountId) continue;
        debit += entry.debit;
        credit += entry.credit;
      }
    }

    return normalSide(account.type) === "debit"
      ? debit - credit
      : credit - debit;
  }

  balances() {
    return Object.fromEntries(
      [...this.accounts.keys()].map((accountId) => [
        accountId,
        this.balance(accountId)
      ])
    );
  }

  trialBalance() {
    let debit = 0;
    let credit = 0;

    for (const transaction of this.transactions) {
      for (const entry of transaction.entries) {
        debit += entry.debit;
        credit += entry.credit;
      }
    }

    return {
      debit,
      credit,
      balanced: debit === credit
    };
  }
}

export function validateTransaction(transaction, accounts) {
  if (!transaction?.id?.trim()) {
    throw new Error("transaction id is required");
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(transaction.date ?? "")) {
    throw new Error("date must use YYYY-MM-DD");
  }

  if (!Array.isArray(transaction.entries) || transaction.entries.length < 2) {
    throw new Error("transaction needs at least two entries");
  }

  let debit = 0;
  let credit = 0;

  for (const entry of transaction.entries) {
    if (!accounts.has(entry.account)) {
      throw new Error(`unknown account: ${entry.account}`);
    }

    if (!Number.isInteger(entry.debit) || !Number.isInteger(entry.credit)) {
      throw new Error("debit and credit must be integer minor units");
    }

    if (entry.debit < 0 || entry.credit < 0) {
      throw new Error("debit and credit cannot be negative");
    }

    const activeSides =
      Number(entry.debit > 0) + Number(entry.credit > 0);

    if (activeSides !== 1) {
      throw new Error("each entry must use exactly one side");
    }

    debit += entry.debit;
    credit += entry.credit;
  }

  if (debit !== credit) {
    throw new Error(
      `transaction is not balanced: debit=${debit}, credit=${credit}`
    );
  }
}
