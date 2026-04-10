/**
 * latest-signals — Get latest intelligence signals.
 * Proxies to delphi-oracle.onrender.com
 */
export default async function handler(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const limit = url.searchParams.get('limit') || '10';
  const upstream = await fetch(`https://delphi-oracle.onrender.com/v1/signals/latest?limit=${limit}`, {
    headers: { 'User-Agent': 'bankr-x402-proxy' }
  });
  const data = await upstream.json();
  return Response.json(data, { status: upstream.status });
}
