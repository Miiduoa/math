import test from "node:test";
import assert from "node:assert/strict";

import { Ledger } from "../src/ledger.js";
import { monthlyIncomeStatement } from "../src/reports.js";

const accounts = [
  { id: "cash", type: "asset" },
  { id: "card", type: "liability" },
  { id: "salary", type: "income" },
  { id: "food", type: "expense" }
];

function makeLedger() {
  return new Ledger(accounts);
}

test("balanced transaction posts", () => {
  const ledger = makeLedger();

  ledger.post({
    id: "1",
    date: "2026-10-01",
    entries: [
      { account: "cash", debit: 10000, credit: 0 },
      { account: "salary", debit: 0, credit: 10000 }
    ]
  });

  assert.equal(ledger.balance("cash"), 10000);
  assert.equal(ledger.balance("salary"), 10000);
});

test("unbalanced transaction is rejected", () => {
  const ledger = makeLedger();

  assert.throws(
    () =>
      ledger.post({
        id: "1",
        date: "2026-10-01",
        entries: [
          { account: "cash", debit: 10000, credit: 0 },
          { account: "salary", debit: 0, credit: 9000 }
        ]
      }),
    /not balanced/
  );
});

test("duplicate transaction id is rejected", () => {
  const ledger = makeLedger();
  const transaction = {
    id: "1",
    date: "2026-10-01",
    entries: [
      { account: "cash", debit: 10000, credit: 0 },
      { account: "salary", debit: 0, credit: 10000 }
    ]
  };

  ledger.post(transaction);
  assert.throws(() => ledger.post(transaction), /duplicate transaction id/);
});

test("liability uses credit-normal balance", () => {
  const ledger = makeLedger();

  ledger.post({
    id: "1",
    date: "2026-10-02",
    entries: [
      { account: "food", debit: 2500, credit: 0 },
      { account: "card", debit: 0, credit: 2500 }
    ]
  });

  assert.equal(ledger.balance("card"), 2500);
  assert.equal(ledger.balance("food"), 2500);
});

test("trial balance remains balanced", () => {
  const ledger = makeLedger();

  ledger.post({
    id: "1",
    date: "2026-10-01",
    entries: [
      { account: "cash", debit: 10000, credit: 0 },
      { account: "salary", debit: 0, credit: 10000 }
    ]
  });

  assert.deepEqual(ledger.trialBalance(), {
    debit: 10000,
    credit: 10000,
    balanced: true
  });
});

test("monthly income statement ignores balance-sheet transfers", () => {
  const ledger = makeLedger();

  ledger.post({
    id: "salary",
    date: "2026-10-01",
    entries: [
      { account: "cash", debit: 100000, credit: 0 },
      { account: "salary", debit: 0, credit: 100000 }
    ]
  });

  ledger.post({
    id: "food",
    date: "2026-10-02",
    entries: [
      { account: "food", debit: 20000, credit: 0 },
      { account: "card", debit: 0, credit: 20000 }
    ]
  });

  ledger.post({
    id: "pay-card",
    date: "2026-10-03",
    entries: [
      { account: "card", debit: 20000, credit: 0 },
      { account: "cash", debit: 0, credit: 20000 }
    ]
  });

  assert.deepEqual(monthlyIncomeStatement(ledger, "2026-10"), {
    month: "2026-10",
    income: 100000,
    expense: 20000,
    netIncome: 80000
  });
});

test("minor units must be integers", () => {
  const ledger = makeLedger();

  assert.throws(
    () =>
      ledger.post({
        id: "1",
        date: "2026-10-01",
        entries: [
          { account: "cash", debit: 10.5, credit: 0 },
          { account: "salary", debit: 0, credit: 10.5 }
        ]
      }),
    /integer minor units/
  );
});
