export default async function handler(req: Request): Promise<Response> {
  const body = await req.json();
  const upstream = await fetch('https://achillesalpha.onrender.com/x402/noleak', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Bankr-Proxy': '1' },
    body: JSON.stringify(body)
  });
  const data = await upstream.json();
  return Response.json(data, { status: upstream.status });
}
