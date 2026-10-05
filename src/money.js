export function formatMinor(amount, currency = "TWD") {
  if (!Number.isInteger(amount)) {
    throw new TypeError("amount must be an integer minor-unit value");
  }

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(amount / 100);
}
