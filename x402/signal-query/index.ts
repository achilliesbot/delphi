/**
 * signal-query — Query DELPHI intelligence signals.
 * Proxies to delphi-oracle.onrender.com
 */
export default async function handler(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const params = url.searchParams.toString();
  const upstream = await fetch(`https://delphi-oracle.onrender.com/v1/signals/query?${params}`, {
    headers: { 'User-Agent': 'bankr-x402-proxy' }
  });
  const data = await upstream.json();
  return Response.json(data, { status: upstream.status });
}
