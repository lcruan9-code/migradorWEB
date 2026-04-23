function ensureHttpUrl(value: string) {
  if (!value) return "";
  if (value.startsWith("http://") || value.startsWith("https://")) {
    return value;
  }
  return `http://${value}`;
}

export function getWorkerUrl() {
  const raw =
    process.env.WORKER_URL ||
    process.env.NEXT_PUBLIC_WORKER_URL ||
    "http://localhost:8080";

  return ensureHttpUrl(raw.replace(/\/$/, ""));
}
