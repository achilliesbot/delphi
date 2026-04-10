/**
 * DELPHI Oracle Daemon
 *
 * Autonomous intelligence gathering agent that continuously
 * monitors the agent economy and produces structured signals.
 *
 * Run alongside server.mjs on EC2 or as a cron job.
 */

import pg from 'pg';
import axios from 'axios';
import * as cheerio from 'cheerio';
import crypto from 'crypto';
import { randomUUID } from 'crypto';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres@localhost/achilles_db',
  max: 5,
});

const ORACLE_SIGNER = process.env.ORACLE_SIGNER_KEY || 'delphi-oracle-v1';

function signSignal(data) {
  return crypto.createHmac('sha256', ORACLE_SIGNER).update(JSON.stringify(data)).digest('hex');
}

function generateSignalId(type) {
  const ts = Date.now().toString(36);
  const rand = randomUUID().slice(0, 8);
  return `dph_${type.replace('/', '-')}_${ts}_${rand}`;
}

async function publishSignal({ type, severity, title, data, confidence, ttl_hours = 48 }) {
  const signalId = generateSignalId(type);
  const expiresAt = new Date(Date.now() + (ttl_hours * 60 * 60 * 1000));
  const signalData = { signal_id: signalId, type, severity, title, data, confidence, timestamp: new Date().toISOString() };
  const signature = signSignal(signalData);

  try {
    await pool.query(
      `INSERT INTO delphi_signals (signal_id, type, severity, title, data, confidence, source, signature, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6, 'delphi-oracle', $7, $8)
       ON CONFLICT (signal_id) DO NOTHING`,
      [signalId, type, severity, title, JSON.stringify(data), confidence, signature, expiresAt]
    );
    console.log(`[ORACLE] Published: ${type} | ${severity} | ${title}`);
    return signalId;
  } catch (e) {
    console.error(`[ORACLE] Failed to publish signal: ${e.message}`);
    return null;
  }
}

// ── Intelligence Sources ────────────────────────────────────────────

async function fetchWithTimeout(url, timeout = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const resp = await axios.get(url, {
      signal: controller.signal,
      headers: { 'User-Agent': 'DELPHI-Oracle/0.1 (intelligence-wire)' },
      timeout
    });
    return resp.data;
  } finally {
    clearTimeout(timer);
  }
}

// 1. Scan x402 endpoint health
async function scanX402Health() {
  console.log('[ORACLE] Scanning x402 endpoint health...');
  const endpoints = [
    { name: 'achillesalpha', url: 'https://achillesalpha.onrender.com/health', service: 'Achilles EP AgentIAM' },
    { name: 'execution-protocol', url: 'https://execution-protocol.onrender.com/health', service: 'Execution Protocol' },
  ];

  for (const ep of endpoints) {
    try {
      const start = Date.now();
      const resp = await fetchWithTimeout(ep.url, 8000);
      const latency = Date.now() - start;

      if (latency > 5000) {
        await publishSignal({
          type: 'api-health/degraded',
          severity: 'medium',
          title: `${ep.service} responding slowly (${latency}ms)`,
          data: { service: ep.service, url: ep.url, latency_ms: latency, status: 'degraded' },
          confidence: 0.9
        });
      }
    } catch (e) {
      await publishSignal({
        type: 'api-health/down',
        severity: 'high',
        title: `${ep.service} is unreachable`,
        data: { service: ep.service, url: ep.url, error: e.message, status: 'down' },
        confidence: 0.95
      });
    }
  }
}

// 2. Scan DeFi yields on Base
async function scanDeFiYields() {
  console.log('[ORACLE] Scanning DeFi yields...');
  try {
    const resp = await fetchWithTimeout('https://yields.llama.fi/pools', 15000);
    if (!resp || !resp.data) return;

    // Filter for Base chain, USDC/ETH pools, high yield
    const basePools = resp.data
      .filter(p => p.chain === 'Base' && p.tvlUsd > 100000)
      .sort((a, b) => b.apy - a.apy)
      .slice(0, 10);

    if (basePools.length > 0) {
      const topPool = basePools[0];
      await publishSignal({
        type: 'market/yield',
        severity: topPool.apy > 20 ? 'high' : 'info',
        title: `Top Base yield: ${topPool.project} ${topPool.symbol} at ${topPool.apy?.toFixed(1)}% APY`,
        data: {
          chain: 'Base',
          top_pools: basePools.slice(0, 5).map(p => ({
            project: p.project,
            symbol: p.symbol,
            apy: p.apy?.toFixed(2),
            tvl_usd: Math.round(p.tvlUsd),
            pool_id: p.pool
          }))
        },
        confidence: 0.85,
        ttl_hours: 4
      });
    }
  } catch (e) {
    console.error('[ORACLE] DeFi yield scan failed:', e.message);
  }
}

// 3. Scan agent ecosystem news
async function scanAgentEcosystem() {
  console.log('[ORACLE] Scanning agent ecosystem...');
  const queries = [
    'AI agent x402 protocol new service launch',
    'autonomous agent Base blockchain USDC',
    'AI agent marketplace new agents April 2026'
  ];

  const query = queries[Math.floor(Math.random() * queries.length)];

  try {
    const searchUrl = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
    const resp = await axios.get(searchUrl, {
      headers: { 'User-Agent': 'DELPHI-Oracle/0.1' },
      timeout: 10000
    });

    const $ = cheerio.load(resp.data);
    const results = [];

    $('.result__body').each((i, el) => {
      if (i >= 5) return false;
      const title = $(el).find('.result__title').text().trim();
      const snippet = $(el).find('.result__snippet').text().trim();
      const url = $(el).find('.result__url').text().trim();
      if (title && snippet) {
        results.push({ title, snippet, url });
      }
    });

    if (results.length > 0) {
      await publishSignal({
        type: 'ecosystem/new-service',
        severity: 'info',
        title: `Agent ecosystem scan: ${results.length} findings for "${query}"`,
        data: {
          query,
          results: results.slice(0, 3),
          scan_time: new Date().toISOString()
        },
        confidence: 0.6,
        ttl_hours: 24
      });
    }
  } catch (e) {
    console.error('[ORACLE] Ecosystem scan failed:', e.message);
  }
}

// 4. Monitor crypto market signals
async function scanMarketSignals() {
  console.log('[ORACLE] Scanning market signals...');
  try {
    const resp = await fetchWithTimeout(
      'https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin,usd-coin&vs_currencies=usd&include_24hr_change=true',
      10000
    );

    if (resp) {
      const eth = resp.ethereum;
      const btc = resp.bitcoin;

      // Alert on significant moves
      if (Math.abs(eth?.usd_24h_change || 0) > 5) {
        await publishSignal({
          type: 'market/price',
          severity: Math.abs(eth.usd_24h_change) > 10 ? 'high' : 'medium',
          title: `ETH ${eth.usd_24h_change > 0 ? 'up' : 'down'} ${Math.abs(eth.usd_24h_change).toFixed(1)}% in 24h ($${eth.usd})`,
          data: {
            asset: 'ETH',
            price_usd: eth.usd,
            change_24h_pct: eth.usd_24h_change?.toFixed(2),
            direction: eth.usd_24h_change > 0 ? 'bullish' : 'bearish'
          },
          confidence: 0.95,
          ttl_hours: 6
        });
      }

      if (Math.abs(btc?.usd_24h_change || 0) > 5) {
        await publishSignal({
          type: 'market/price',
          severity: Math.abs(btc.usd_24h_change) > 10 ? 'high' : 'medium',
          title: `BTC ${btc.usd_24h_change > 0 ? 'up' : 'down'} ${Math.abs(btc.usd_24h_change).toFixed(1)}% in 24h ($${btc.usd})`,
          data: {
            asset: 'BTC',
            price_usd: btc.usd,
            change_24h_pct: btc.usd_24h_change?.toFixed(2),
            direction: btc.usd_24h_change > 0 ? 'bullish' : 'bearish'
          },
          confidence: 0.95,
          ttl_hours: 6
        });
      }

      // Always publish a market snapshot
      await publishSignal({
        type: 'market/price',
        severity: 'info',
        title: `Market snapshot: ETH $${eth?.usd} | BTC $${btc?.usd}`,
        data: {
          ethereum: { price: eth?.usd, change_24h: eth?.usd_24h_change?.toFixed(2) + '%' },
          bitcoin: { price: btc?.usd, change_24h: btc?.usd_24h_change?.toFixed(2) + '%' }
        },
        confidence: 0.99,
        ttl_hours: 1
      });
    }
  } catch (e) {
    console.error('[ORACLE] Market scan failed:', e.message);
  }
}

// 5. CDP x402 ecosystem scan
async function scanX402Ecosystem() {
  console.log('[ORACLE] Scanning x402 ecosystem...');
  try {
    const resp = await fetchWithTimeout(
      'https://api.cdp.coinbase.com/platform/v2/x402/discovery/resources?limit=5&offset=' + Math.floor(Math.random() * 100),
      10000
    );

    if (resp && resp.items) {
      const total = resp.pagination?.total || 0;
      const newServices = resp.items.map(item => ({
        resource: item.resource,
        description: item.accepts?.[0]?.description?.slice(0, 100),
        price: item.accepts?.[0]?.maxAmountRequired,
        network: item.accepts?.[0]?.network
      }));

      await publishSignal({
        type: 'ecosystem/new-service',
        severity: 'info',
        title: `x402 ecosystem: ${total} total services on CDP Discovery`,
        data: {
          total_services: total,
          sample_services: newServices,
          source: 'cdp-discovery-api'
        },
        confidence: 0.9,
        ttl_hours: 12
      });
    }
  } catch (e) {
    console.error('[ORACLE] x402 ecosystem scan failed:', e.message);
  }
}

// ── Oracle Run Cycle ────────────────────────────────────────────────
async function runOracleCycle() {
  console.log(`\n[ORACLE] === Cycle starting at ${new Date().toISOString()} ===`);

  // Run all scans with error isolation
  const scans = [
    { name: 'x402-health', fn: scanX402Health },
    { name: 'defi-yields', fn: scanDeFiYields },
    { name: 'market-signals', fn: scanMarketSignals },
    { name: 'agent-ecosystem', fn: scanAgentEcosystem },
    { name: 'x402-ecosystem', fn: scanX402Ecosystem },
  ];

  for (const scan of scans) {
    try {
      await scan.fn();
    } catch (e) {
      console.error(`[ORACLE] Scan ${scan.name} failed:`, e.message);
    }
  }

  // Clean expired signals
  try {
    const cleaned = await pool.query('DELETE FROM delphi_signals WHERE expires_at < NOW()');
    if (cleaned.rowCount > 0) {
      console.log(`[ORACLE] Cleaned ${cleaned.rowCount} expired signals`);
    }
  } catch (e) { /* non-critical */ }

  const count = await pool.query('SELECT COUNT(*) FROM delphi_signals WHERE expires_at IS NULL OR expires_at > NOW()');
  console.log(`[ORACLE] === Cycle complete. Active signals: ${count.rows[0].count} ===\n`);
}

// ── Main ────────────────────────────────────────────────────────────
async function main() {
  console.log('[DELPHI ORACLE] Autonomous intelligence daemon starting...');

  // Run immediately
  await runOracleCycle();

  // Then run every 15 minutes
  const INTERVAL_MS = 15 * 60 * 1000;
  setInterval(async () => {
    try {
      await runOracleCycle();
    } catch (e) {
      console.error('[ORACLE] Cycle error:', e.message);
    }
  }, INTERVAL_MS);

  console.log(`[DELPHI ORACLE] Running every ${INTERVAL_MS / 60000} minutes`);
}

// Support single-run mode for cron
if (process.argv.includes('--once')) {
  runOracleCycle().then(() => {
    console.log('[ORACLE] Single run complete.');
    process.exit(0);
  });
} else {
  main().catch(e => {
    console.error('[ORACLE] Fatal:', e.message);
    process.exit(1);
  });
}
