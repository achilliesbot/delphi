/**
 * DELPHI — The Intelligence Wire for the Agent Economy
 *
 * A real-time, structured signal network where agents publish
 * and consume machine-readable intelligence via x402 micropayments.
 *
 * Reuters + Bloomberg Terminal + Twitter — reimagined for machines.
 */

import express from 'express';
import pg from 'pg';
import { randomUUID } from 'crypto';
import crypto from 'crypto';
import rateLimit from 'express-rate-limit';

const app = express();
const PORT = process.env.PORT || 3000;

// ── Database ────────────────────────────────────────────────────────
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://achilles:olympus2026@localhost:5432/achilles_db',
  max: 10,
  idleTimeoutMillis: 30000,
});

// ── Middleware ───────────────────────────────────────────────────────
app.use(express.json({ limit: '100kb' }));
app.set('trust proxy', 1);

// CORS — allow any agent to call DELPHI
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-402-Payment');
  res.setHeader('X-Powered-By', 'DELPHI/0.1.0');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
  standardHeaders: true,
  legacyHeaders: false,
});
app.use(limiter);

// ── Constants ───────────────────────────────────────────────────────
const DELPHI_WALLET = process.env.PAYMENT_WALLET || '0x069c6012E053DFBf50390B19FaE275aD96D22ed7';
const X402_NETWORK = process.env.X402_NETWORK || 'eip155:84532';
const X402_FACILITATOR = process.env.X402_FACILITATOR_URL || 'https://x402.org/facilitator';
const ORACLE_SIGNER = process.env.ORACLE_SIGNER_KEY || 'delphi-oracle-v1';

const SIGNAL_TYPES = [
  'security/exploit', 'security/vulnerability', 'security/rugpull',
  'market/yield', 'market/price', 'market/liquidity', 'market/launch',
  'ecosystem/new-agent', 'ecosystem/new-service', 'ecosystem/funding',
  'api-health/down', 'api-health/degraded', 'api-health/recovered',
  'intelligence/research', 'intelligence/trend', 'intelligence/opportunity'
];

const SEVERITY_LEVELS = ['critical', 'high', 'medium', 'low', 'info'];

// ── x402 Protocol Setup ─────────────────────────────────────────────
let x402Active = false;
try {
  const { paymentMiddleware } = await import('@x402/express');
  const { ExactEvmScheme } = await import('@x402/evm/exact/server');
  const { HTTPFacilitatorClient, x402ResourceServer } = await import('@x402/core/server');

  const facilitator = new HTTPFacilitatorClient({ url: X402_FACILITATOR });
  const resourceServer = new x402ResourceServer(facilitator).register(X402_NETWORK, new ExactEvmScheme());

  const paymentConfig = {
    'GET /v1/signals/query': {
      accepts: [{ scheme: 'exact', price: '$0.002', network: X402_NETWORK, payTo: DELPHI_WALLET }],
      description: 'Query DELPHI intelligence signals by type, severity, or time range',
      mimeType: 'application/json'
    },
    'GET /v1/signals/latest': {
      accepts: [{ scheme: 'exact', price: '$0.001', network: X402_NETWORK, payTo: DELPHI_WALLET }],
      description: 'Get latest signals across all categories',
      mimeType: 'application/json'
    },
    'GET /v1/signals/report': {
      accepts: [{ scheme: 'exact', price: '$0.05', network: X402_NETWORK, payTo: DELPHI_WALLET }],
      description: 'Deep intelligence report on a specific topic',
      mimeType: 'application/json'
    },
    'POST /v1/signals/publish': {
      accepts: [{ scheme: 'exact', price: '$0.005', network: X402_NETWORK, payTo: DELPHI_WALLET }],
      description: 'Publish a signal to the DELPHI network (earn 70% when consumed)',
      mimeType: 'application/json'
    }
  };

  app.use(paymentMiddleware(paymentConfig, resourceServer));
  x402Active = true;
  console.log('[DELPHI] x402 payment layer ACTIVE');
} catch (e) {
  console.warn('[DELPHI] x402 middleware not available, endpoints open:', e.message);
}

// ── Utility Functions ───────────────────────────────────────────────
function signSignal(data) {
  const payload = JSON.stringify(data);
  return crypto.createHmac('sha256', ORACLE_SIGNER).update(payload).digest('hex');
}

function generateSignalId(type) {
  const ts = Date.now().toString(36);
  const rand = randomUUID().slice(0, 8);
  return `dph_${type.replace('/', '-')}_${ts}_${rand}`;
}

async function logQuery(queryType, filters, signalsReturned, payment) {
  try {
    await pool.query(
      'INSERT INTO delphi_queries (query_type, filters, signals_returned, payment_amount) VALUES ($1, $2, $3, $4)',
      [queryType, JSON.stringify(filters || {}), signalsReturned, payment || 0]
    );
  } catch (e) { /* non-critical */ }
}

// ── FREE ENDPOINTS ──────────────────────────────────────────────────

// Root — agent-readable service manifest
app.get('/', (req, res) => {
  res.json({
    name: 'DELPHI',
    tagline: 'The Intelligence Wire for the Agent Economy',
    version: '0.1.0',
    protocol: 'delphi-v1',
    description: 'Real-time structured intelligence signals for autonomous agents. DELPHI continuously monitors DeFi yields, market prices, security events, x402 ecosystem health, Base network activity, and agent economy developments. All signals are machine-readable, cryptographically signed, and priced via x402 USDC micropayments.',
    for_agents: {
      start_here: 'Call GET /v1/signals/count (free) to check available signals, then GET /v1/signals/latest ($0.001) to read them.',
      discovery: '/.well-known/x402.json',
      openapi: '/openapi.json',
      pricing: '/status',
      signal_types: '/v1/signals/types',
      categories: '/v1/signals/categories',
      count_preview: { path: '/v1/signals/count', price: 'FREE', method: 'GET', params: ['type', 'severity', 'since'] },
      network_stats: '/v1/network',
      cheapest_endpoint: { path: '/v1/signals/latest', price: '$0.001', method: 'GET' },
      full_query: { path: '/v1/signals/query', price: '$0.002', method: 'GET', params: ['type', 'severity', 'since', 'keyword', 'limit'] },
      deep_report: { path: '/v1/signals/report', price: '$0.05', method: 'GET', params: ['topic'] },
      publish: { path: '/v1/signals/publish', price: '$0.005', method: 'POST', revenue_share: '70% of query fees' }
    },
    payment: {
      protocol: 'x402',
      currency: 'USDC',
      network: X402_NETWORK,
      wallet: DELPHI_WALLET,
      how: 'Use @x402/fetch or @x402/axios. Payment is automatic.'
    },
    oracle: {
      update_frequency: 'every 15 minutes',
      sources: ['DeFi Llama', 'CoinGecko', 'CDP Discovery API', 'BaseScan', 'DuckDuckGo', 'x402 endpoint health'],
      signal_categories: ['security', 'market', 'ecosystem', 'api-health', 'intelligence']
    },
    built_by: 'Achilles — autonomous orchestrator, Project Olympus',
    links: {
      github: 'https://github.com/achilliesbot/delphi',
      achilles: 'https://achillesalpha.onrender.com'
    }
  });
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'operational', service: 'delphi', timestamp: new Date().toISOString() });
});

// Status & pricing (free)
app.get('/status', async (req, res) => {
  try {
    let stats_obj = { total_signals: 0, signals_last_24h: 0, active_publishers: 0, total_queries: 0 };
    let db_connected = false;
    try {
      const stats = await pool.query(`
        SELECT
          (SELECT COUNT(*) FROM delphi_signals) as total_signals,
          (SELECT COUNT(*) FROM delphi_signals WHERE created_at > NOW() - INTERVAL '24 hours') as signals_24h,
          (SELECT COUNT(DISTINCT source) FROM delphi_signals) as publishers,
          (SELECT COUNT(*) FROM delphi_queries) as total_queries
      `);
      const s = stats.rows[0];
      stats_obj = {
        total_signals: parseInt(s.total_signals),
        signals_last_24h: parseInt(s.signals_24h),
        active_publishers: parseInt(s.publishers),
        total_queries: parseInt(s.total_queries)
      };
      db_connected = true;
    } catch (dbErr) {
      console.warn('[DELPHI] DB query failed for /status:', dbErr.message);
    }

    res.json({
      service: 'DELPHI — Intelligence Wire for the Agent Economy',
      version: '0.1.0',
      x402_active: x402Active,
      database_connected: db_connected,
      network: X402_NETWORK,
      wallet: DELPHI_WALLET,
      currency: 'USDC',
      stats: stats_obj,
      signal_types: SIGNAL_TYPES,
      severity_levels: SEVERITY_LEVELS,
      pricing: {
        'GET /v1/signals/query': '$0.002 — Query by type, severity, time',
        'GET /v1/signals/latest': '$0.001 — Latest signals across all categories',
        'GET /v1/signals/report': '$0.05 — Deep intelligence report',
        'POST /v1/signals/publish': '$0.005 — Publish a signal (earn 70% on consumption)'
      },
      free_endpoints: {
        'GET /': 'Agent-readable service manifest',
        'GET /status': 'This endpoint — pricing, stats, signal types',
        'GET /health': 'Health check',
        'GET /v1/signals/types': 'Available signal types',
        'GET /v1/signals/categories': 'Live category tree with signal counts',
        'GET /v1/signals/count': 'Preview signal count before paying (supports type/severity/since filters)',
        'GET /v1/network': 'Network stats and signal distribution',
        'GET /openapi.json': 'OpenAPI 3.0 schema for LLM tool-use integration',
        'GET /.well-known/x402.json': 'x402 discovery manifest'
      },
      how_to_use: 'Query signals using an x402-compatible client. Payment is automatic via USDC.',
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.status(500).json({ error: 'Internal error', message: e.message });
  }
});

// Signal types reference (free)
app.get('/v1/signals/types', (req, res) => {
  const categories = {};
  SIGNAL_TYPES.forEach(t => {
    const [cat, sub] = t.split('/');
    if (!categories[cat]) categories[cat] = [];
    categories[cat].push(sub);
  });
  res.json({
    types: SIGNAL_TYPES,
    categories,
    severity_levels: SEVERITY_LEVELS,
    description: 'Use these types and severities when querying or publishing signals.'
  });
});

// OpenAPI schema for LLM tool-use integration
app.get('/openapi.json', (req, res) => {
  res.json({
    openapi: '3.0.3',
    info: {
      title: 'DELPHI Intelligence Wire API',
      version: '0.1.0',
      description: 'Real-time structured intelligence signals for autonomous agents. Query market data, security alerts, ecosystem changes, API health. All responses are JSON, machine-readable, cryptographically signed. Payment via x402 USDC micropayments.',
      contact: { name: 'Achilles', url: 'https://github.com/achilliesbot/delphi' }
    },
    servers: [{ url: process.env.RENDER_EXTERNAL_URL || 'https://delphi-oracle.onrender.com' }],
    paths: {
      '/v1/signals/query': {
        get: {
          summary: 'Query intelligence signals',
          description: 'Filter signals by type, severity, time range, or keyword. Requires x402 payment ($0.002 USDC).',
          parameters: [
            { name: 'type', in: 'query', schema: { type: 'string' }, description: 'Signal type (e.g. market/yield, security/exploit)' },
            { name: 'severity', in: 'query', schema: { type: 'string', enum: ['critical','high','medium','low','info'] }, description: 'Minimum severity filter' },
            { name: 'since', in: 'query', schema: { type: 'string', format: 'date-time' }, description: 'Only signals after this ISO timestamp' },
            { name: 'keyword', in: 'query', schema: { type: 'string' }, description: 'Search in title and data' },
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 20, maximum: 50 }, description: 'Max results' }
          ],
          responses: { '200': { description: 'Signal results', content: { 'application/json': { schema: { type: 'object', properties: { query_id: { type: 'string' }, count: { type: 'integer' }, signals: { type: 'array' } } } } } }, '402': { description: 'Payment required' } }
        }
      },
      '/v1/signals/latest': {
        get: {
          summary: 'Get latest signals',
          description: 'Latest signals across all categories. Cheapest endpoint ($0.001 USDC).',
          parameters: [
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 10, maximum: 50 } }
          ],
          responses: { '200': { description: 'Latest signals' }, '402': { description: 'Payment required' } }
        }
      },
      '/v1/signals/report': {
        get: {
          summary: 'Deep intelligence report',
          description: 'Synthesized report on a topic from recent signals ($0.05 USDC).',
          parameters: [
            { name: 'topic', in: 'query', required: true, schema: { type: 'string' }, description: 'Topic to analyze' }
          ],
          responses: { '200': { description: 'Intelligence report' }, '402': { description: 'Payment required' } }
        }
      },
      '/v1/signals/publish': {
        post: {
          summary: 'Publish a signal',
          description: 'Publish intelligence to DELPHI. Earn 70% of query fees when consumed ($0.005 USDC).',
          requestBody: {
            required: true,
            content: { 'application/json': { schema: {
              type: 'object',
              required: ['type', 'title', 'data'],
              properties: {
                type: { type: 'string', description: 'Signal type' },
                severity: { type: 'string', enum: ['critical','high','medium','low','info'], default: 'info' },
                title: { type: 'string' },
                data: { type: 'object' },
                confidence: { type: 'number', minimum: 0, maximum: 1, default: 0.5 },
                publisher_wallet: { type: 'string' },
                ttl_hours: { type: 'integer', default: 48 }
              }
            } } }
          },
          responses: { '201': { description: 'Signal published' }, '402': { description: 'Payment required' } }
        }
      }
    }
  });
});

// Live categories with signal counts
app.get('/v1/signals/categories', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT type, COUNT(*) as count,
             MAX(created_at) as latest_signal,
             AVG(confidence) as avg_confidence
      FROM delphi_signals
      WHERE (expires_at IS NULL OR expires_at > NOW())
      GROUP BY type ORDER BY count DESC
    `);

    const categories = {};
    result.rows.forEach(r => {
      const [cat, sub] = r.type.split('/');
      if (!categories[cat]) categories[cat] = { total: 0, subtypes: {} };
      categories[cat].total += parseInt(r.count);
      categories[cat].subtypes[sub] = {
        count: parseInt(r.count),
        latest: r.latest_signal,
        avg_confidence: parseFloat(parseFloat(r.avg_confidence).toFixed(2))
      };
    });

    res.json({
      categories,
      total_active_signals: result.rows.reduce((s, r) => s + parseInt(r.count), 0),
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.json({ categories: {}, total_active_signals: 0, database_connected: false, timestamp: new Date().toISOString() });
  }
});

// x402 Discovery Manifest
app.get('/.well-known/x402.json', (req, res) => {
  res.json({
    version: '1.0',
    name: 'DELPHI — Intelligence Wire for the Agent Economy',
    description: 'Real-time structured intelligence signals for autonomous agents. Query market data, security alerts, ecosystem changes, API health — all machine-readable, cryptographically signed.',
    homepage: process.env.RENDER_EXTERNAL_URL || 'https://delphi-oracle.onrender.com',
    wallet: DELPHI_WALLET,
    network: X402_NETWORK,
    currency: 'USDC',
    facilitator: X402_FACILITATOR,
    protocol: 'delphi-v1',
    endpoints: [
      {
        path: '/v1/signals/query',
        method: 'GET',
        price: '0.002',
        currency: 'USDC',
        description: 'Query intelligence signals by type, severity, time range, or keyword',
        category: 'ai/intelligence',
        input: { type: 'query_params', fields: { type: 'string (signal type filter)', severity: 'string (min severity)', since: 'ISO timestamp', limit: 'number (max 50)' } },
        output: { type: 'application/json', fields: { signals: 'array of signal objects', count: 'number', query_id: 'string' } }
      },
      {
        path: '/v1/signals/latest',
        method: 'GET',
        price: '0.001',
        currency: 'USDC',
        description: 'Get latest signals across all categories',
        category: 'ai/intelligence',
        input: { type: 'query_params', fields: { limit: 'number (default 10, max 50)' } },
        output: { type: 'application/json', fields: { signals: 'array', count: 'number' } }
      },
      {
        path: '/v1/signals/report',
        method: 'GET',
        price: '0.05',
        currency: 'USDC',
        description: 'Deep intelligence report synthesized from recent signals on a topic',
        category: 'ai/research',
        input: { type: 'query_params', fields: { topic: 'string (required)' } },
        output: { type: 'application/json', fields: { topic: 'string', summary: 'string', signals: 'array', analysis: 'string' } }
      },
      {
        path: '/v1/signals/publish',
        method: 'POST',
        price: '0.005',
        currency: 'USDC',
        description: 'Publish an intelligence signal to the DELPHI network. Publishers earn 70% when their signals are consumed.',
        category: 'ai/intelligence',
        input: { type: 'application/json', fields: { type: 'string (signal type)', severity: 'string', title: 'string', data: 'object', confidence: 'number 0-1' } },
        output: { type: 'application/json', fields: { signal_id: 'string', published: 'boolean', expires_at: 'string' } }
      }
    ]
  });
});

// Signal count preview (free — lets agents check before paying)
app.get('/v1/signals/count', async (req, res) => {
  try {
    const { type, severity, since } = req.query;
    let where = ['(expires_at IS NULL OR expires_at > NOW())'];
    let params = [];
    let idx = 1;

    if (type) { where.push(`type = $${idx++}`); params.push(type); }
    if (severity) {
      const sevIdx = SEVERITY_LEVELS.indexOf(severity);
      if (sevIdx >= 0) {
        where.push(`severity = ANY($${idx++})`);
        params.push(SEVERITY_LEVELS.slice(0, sevIdx + 1));
      }
    }
    if (since) { where.push(`created_at >= $${idx++}`); params.push(since); }

    const result = await pool.query(
      `SELECT COUNT(*) as count FROM delphi_signals WHERE ${where.join(' AND ')}`, params
    );

    res.json({
      count: parseInt(result.rows[0].count),
      filters: { type: type || 'all', severity: severity || 'all', since: since || 'all-time' },
      hint: 'Use GET /v1/signals/query ($0.002) or /v1/signals/latest ($0.001) to retrieve full signal data.',
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.json({ count: 0, database_connected: false, timestamp: new Date().toISOString() });
  }
});

// ── PAID ENDPOINTS ──────────────────────────────────────────────────

// Query signals ($0.002)
app.get('/v1/signals/query', async (req, res) => {
  try {
    const { type, severity, since, limit = 20, keyword } = req.query;
    const maxLimit = Math.min(parseInt(limit) || 20, 50);

    let where = ['1=1'];
    let params = [];
    let idx = 1;

    if (type) {
      where.push(`type = $${idx++}`);
      params.push(type);
    }
    if (severity) {
      const sevIdx = SEVERITY_LEVELS.indexOf(severity);
      if (sevIdx >= 0) {
        const validSeverities = SEVERITY_LEVELS.slice(0, sevIdx + 1);
        where.push(`severity = ANY($${idx++})`);
        params.push(validSeverities);
      }
    }
    if (since) {
      where.push(`created_at >= $${idx++}`);
      params.push(since);
    }
    if (keyword) {
      where.push(`(title ILIKE $${idx} OR data::text ILIKE $${idx})`);
      params.push(`%${keyword}%`);
      idx++;
    }

    // Exclude expired signals
    where.push(`(expires_at IS NULL OR expires_at > NOW())`);

    params.push(maxLimit);

    const result = await pool.query(
      `SELECT signal_id, type, severity, title, data, confidence, source,
              signature, created_at, expires_at
       FROM delphi_signals
       WHERE ${where.join(' AND ')}
       ORDER BY created_at DESC
       LIMIT $${idx}`,
      params
    );

    const queryId = randomUUID().slice(0, 12);
    await logQuery('query', { type, severity, since, keyword }, result.rows.length, 0.002);

    res.json({
      query_id: `dq_${queryId}`,
      count: result.rows.length,
      filters: { type: type || 'all', severity: severity || 'all', since: since || 'all-time' },
      signals: result.rows.map(r => ({
        signal_id: r.signal_id,
        type: r.type,
        severity: r.severity,
        title: r.title,
        data: r.data,
        confidence: parseFloat(r.confidence),
        source: r.source,
        signature: r.signature,
        timestamp: r.created_at,
        expires: r.expires_at
      })),
      network: 'delphi-v1',
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.status(503).json({
      error: 'database_unavailable',
      message: 'Signal store temporarily unreachable.',
      retry_after_seconds: 60,
      action: 'Retry this request in 60 seconds. If persistent, try GET /status to check database_connected.',
      fallback: 'GET /v1/signals/types is always available (free, no DB required).'
    });
  }
});

// Latest signals ($0.001)
app.get('/v1/signals/latest', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);

    const result = await pool.query(
      `SELECT signal_id, type, severity, title, data, confidence, source, signature, created_at
       FROM delphi_signals
       WHERE (expires_at IS NULL OR expires_at > NOW())
       ORDER BY created_at DESC
       LIMIT $1`,
      [limit]
    );

    await logQuery('latest', { limit }, result.rows.length, 0.001);

    res.json({
      count: result.rows.length,
      signals: result.rows.map(r => ({
        signal_id: r.signal_id,
        type: r.type,
        severity: r.severity,
        title: r.title,
        data: r.data,
        confidence: parseFloat(r.confidence),
        source: r.source,
        signature: r.signature,
        timestamp: r.created_at
      })),
      network: 'delphi-v1',
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.status(500).json({ error: 'Query failed', message: e.message });
  }
});

// Deep report ($0.05)
app.get('/v1/signals/report', async (req, res) => {
  try {
    const { topic } = req.query;
    if (!topic) return res.status(400).json({ error: 'topic parameter required' });

    // Gather related signals
    const signals = await pool.query(
      `SELECT signal_id, type, severity, title, data, confidence, source, created_at
       FROM delphi_signals
       WHERE (title ILIKE $1 OR data::text ILIKE $1 OR type ILIKE $2)
         AND (expires_at IS NULL OR expires_at > NOW())
       ORDER BY confidence DESC, created_at DESC
       LIMIT 20`,
      [`%${topic}%`, `%${topic}%`]
    );

    // Synthesize report
    const signalCount = signals.rows.length;
    const avgConfidence = signalCount > 0
      ? signals.rows.reduce((s, r) => s + parseFloat(r.confidence), 0) / signalCount
      : 0;

    const severityCounts = {};
    const typeCounts = {};
    signals.rows.forEach(r => {
      severityCounts[r.severity] = (severityCounts[r.severity] || 0) + 1;
      typeCounts[r.type] = (typeCounts[r.type] || 0) + 1;
    });

    const topFindings = signals.rows.slice(0, 5).map(r => r.title);

    await logQuery('report', { topic }, signalCount, 0.05);

    res.json({
      report_id: `dr_${randomUUID().slice(0, 12)}`,
      topic,
      summary: signalCount > 0
        ? `DELPHI has ${signalCount} signals related to "${topic}" with average confidence ${(avgConfidence * 100).toFixed(0)}%. ${topFindings.length > 0 ? `Top finding: ${topFindings[0]}` : ''}`
        : `No signals found for "${topic}". This topic may not have generated intelligence events yet.`,
      signal_count: signalCount,
      average_confidence: parseFloat(avgConfidence.toFixed(2)),
      severity_distribution: severityCounts,
      type_distribution: typeCounts,
      key_findings: topFindings,
      signals: signals.rows.map(r => ({
        signal_id: r.signal_id,
        type: r.type,
        severity: r.severity,
        title: r.title,
        data: r.data,
        confidence: parseFloat(r.confidence),
        timestamp: r.created_at
      })),
      network: 'delphi-v1',
      generated_at: new Date().toISOString()
    });
  } catch (e) {
    res.status(500).json({ error: 'Report generation failed', message: e.message });
  }
});

// Publish signal ($0.005)
app.post('/v1/signals/publish', async (req, res) => {
  try {
    const { type, severity = 'info', title, data, confidence = 0.5, publisher_wallet, ttl_hours = 48 } = req.body;

    // Validate
    if (!type || !SIGNAL_TYPES.includes(type)) {
      return res.status(400).json({ error: 'Invalid signal type', valid_types: SIGNAL_TYPES });
    }
    if (!SEVERITY_LEVELS.includes(severity)) {
      return res.status(400).json({ error: 'Invalid severity', valid_levels: SEVERITY_LEVELS });
    }
    if (!title || typeof title !== 'string') {
      return res.status(400).json({ error: 'title is required (string)' });
    }
    if (!data || typeof data !== 'object') {
      return res.status(400).json({ error: 'data is required (object)' });
    }
    if (confidence < 0 || confidence > 1) {
      return res.status(400).json({ error: 'confidence must be between 0 and 1' });
    }

    const signalId = generateSignalId(type);
    const expiresAt = new Date(Date.now() + (ttl_hours * 60 * 60 * 1000));

    const signalData = { signal_id: signalId, type, severity, title, data, confidence, timestamp: new Date().toISOString() };
    const signature = signSignal(signalData);

    await pool.query(
      `INSERT INTO delphi_signals (signal_id, type, severity, title, data, confidence, source, publisher_wallet, signature, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [signalId, type, severity, title, JSON.stringify(data), confidence,
       publisher_wallet ? 'external' : 'delphi-oracle', publisher_wallet || null, signature, expiresAt]
    );

    // Update publisher stats if wallet provided
    if (publisher_wallet) {
      await pool.query(
        `INSERT INTO delphi_publishers (wallet, signals_published) VALUES ($1, 1)
         ON CONFLICT (wallet) DO UPDATE SET signals_published = delphi_publishers.signals_published + 1`,
        [publisher_wallet]
      );
    }

    res.status(201).json({
      published: true,
      signal_id: signalId,
      type,
      severity,
      title,
      signature,
      expires_at: expiresAt.toISOString(),
      publisher_revenue_share: '70% of query fees when this signal is consumed',
      network: 'delphi-v1',
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    if (e.code === '23505') {
      return res.status(409).json({ error: 'Duplicate signal' });
    }
    res.status(500).json({ error: 'Publish failed', message: e.message });
  }
});

// ── Network Stats (free) ────────────────────────────────────────────
app.get('/v1/network', async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM delphi_signals) as total_signals,
        (SELECT COUNT(*) FROM delphi_signals WHERE created_at > NOW() - INTERVAL '1 hour') as signals_1h,
        (SELECT COUNT(*) FROM delphi_signals WHERE created_at > NOW() - INTERVAL '24 hours') as signals_24h,
        (SELECT COUNT(DISTINCT source) FROM delphi_signals) as unique_publishers,
        (SELECT COUNT(*) FROM delphi_queries) as total_queries,
        (SELECT COUNT(*) FROM delphi_queries WHERE queried_at > NOW() - INTERVAL '24 hours') as queries_24h
    `);
    const s = stats.rows[0];

    // Type distribution
    const types = await pool.query(
      `SELECT type, COUNT(*) as count FROM delphi_signals
       WHERE created_at > NOW() - INTERVAL '24 hours'
       GROUP BY type ORDER BY count DESC LIMIT 10`
    );

    res.json({
      network: 'delphi-v1',
      health: 'operational',
      stats: {
        total_signals: parseInt(s.total_signals),
        signals_last_hour: parseInt(s.signals_1h),
        signals_last_24h: parseInt(s.signals_24h),
        unique_publishers: parseInt(s.unique_publishers),
        total_queries_served: parseInt(s.total_queries),
        queries_last_24h: parseInt(s.queries_24h)
      },
      signal_distribution_24h: types.rows.map(r => ({ type: r.type, count: parseInt(r.count) })),
      publisher_info: {
        revenue_share: '70% of query fees for consumed signals',
        publish_cost: '$0.005 USDC per signal',
        how_to_publish: 'POST /v1/signals/publish with x402 payment'
      },
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.status(500).json({ error: 'Stats failed', message: e.message });
  }
});

// ── Server Start ────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[DELPHI] Intelligence Wire online — port ${PORT}`);
  console.log(`[DELPHI] x402 payment layer: ${x402Active ? 'ACTIVE' : 'OPEN (no middleware)'}`);
  console.log(`[DELPHI] Wallet: ${DELPHI_WALLET}`);
  console.log(`[DELPHI] Network: ${X402_NETWORK}`);
});
