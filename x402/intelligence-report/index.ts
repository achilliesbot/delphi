/**
 * intelligence-report — Deep report on a topic.
 * Proxies to delphi-oracle.onrender.com
 */
export default async function handler(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const topic = url.searchParams.get('topic') || '';
  if (!topic) return Response.json({ error: 'topic parameter required' }, { status: 400 });
  const upstream = await fetch(`https://delphi-oracle.onrender.com/v1/signals/report?topic=${encodeURIComponent(topic)}`, {
    headers: { 'User-Agent': 'bankr-x402-proxy' }
  });
  const data = await upstream.json();
  return Response.json(data, { status: upstream.status });
}
