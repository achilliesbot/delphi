/**
 * publish-signal — Publish intelligence to DELPHI network.
 * Proxies to delphi-oracle.onrender.com
 */
export default async function handler(req: Request): Promise<Response> {
  if (req.method !== "POST") {
    return Response.json({ error: "POST required" }, { status: 405 });
  }
  const body = await req.json();
  const upstream = await fetch('https://delphi-oracle.onrender.com/v1/signals/publish', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'User-Agent': 'bankr-x402-proxy' },
    body: JSON.stringify(body)
  });
  const data = await upstream.json();
  return Response.json(data, { status: upstream.status });
}
