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
import { importPKCS8, SignJWT } from 'jose';

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
const X402_NETWORK = process.env.X402_NETWORK || 'eip155:8453';
const ORACLE_SIGNER = process.env.ORACLE_SIGNER_KEY || 'delphi-oracle-v1';

// ── Knowledge Graph Init ───────────────────────────────────────────
async function initKnowledgeGraph() {
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS kg_entities (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, type TEXT DEFAULT 'unknown',
      properties JSONB DEFAULT '{}', created_at TIMESTAMPTZ DEFAULT NOW()
    )`);
    await pool.query(`CREATE TABLE IF NOT EXISTS kg_triples (
      id TEXT PRIMARY KEY, subject TEXT NOT NULL REFERENCES kg_entities(id) ON DELETE CASCADE,
      predicate TEXT NOT NULL, object TEXT NOT NULL REFERENCES kg_entities(id) ON DELETE CASCADE,
      valid_from TIMESTAMPTZ, valid_to TIMESTAMPTZ, confidence REAL DEFAULT 1.0,
      source_signal TEXT, extracted_at TIMESTAMPTZ DEFAULT NOW()
    )`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_kg_subject ON kg_triples(subject)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_kg_object ON kg_triples(object)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_kg_predicate ON kg_triples(predicate)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_kg_valid ON kg_triples(valid_from, valid_to)`);
    await pool.query(`CREATE TABLE IF NOT EXISTS kg_contradictions (
      id TEXT PRIMARY KEY, triple_a TEXT REFERENCES kg_triples(id),
      triple_b TEXT REFERENCES kg_triples(id), description TEXT,
      severity TEXT DEFAULT 'medium', resolved BOOLEAN DEFAULT false,
      detected_at TIMESTAMPTZ DEFAULT NOW()
    )`);
    console.log('[DELPHI] Knowledge graph tables ready');
  } catch (e) {
    console.error('[DELPHI] KG init FAILED:', e.message, e.stack);
  }
}
// Init KG after confirming DB connection
pool.query('SELECT 1').then(() => initKnowledgeGraph()).catch(e => console.warn('[DELPHI] DB not ready for KG init:', e.message));

// ── Knowledge Graph Helpers ────────────────────────────────────────
function entityId(name) {
  return name.toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/_+/g, '_').slice(0, 128);
}

async function ensureEntity(name, type = 'unknown', properties = {}) {
  const eid = entityId(name);
  await pool.query(
    `INSERT INTO kg_entities (id, name, type, properties) VALUES ($1, $2, $3, $4)
     ON CONFLICT (id) DO UPDATE SET type = COALESCE(NULLIF($3, 'unknown'), kg_entities.type),
     properties = kg_entities.properties || $4`,
    [eid, name, type, JSON.stringify(properties)]
  );
  return eid;
}

async function addTriple(subject, predicate, object, opts = {}) {
  const subId = await ensureEntity(subject, opts.subjectType);
  const objId = await ensureEntity(object, opts.objectType);
  const pred = predicate.toLowerCase().replace(/\s+/g, '_');
  const tripleId = `t_${subId}_${pred}_${objId}_${Date.now().toString(36)}`;

  // Check for existing identical active triple
  const existing = await pool.query(
    `SELECT id FROM kg_triples WHERE subject=$1 AND predicate=$2 AND object=$3 AND valid_to IS NULL LIMIT 1`,
    [subId, pred, objId]
  );
  if (existing.rows.length > 0) return existing.rows[0].id;

  await pool.query(
    `INSERT INTO kg_triples (id, subject, predicate, object, valid_from, valid_to, confidence, source_signal)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [tripleId, subId, pred, objId, opts.validFrom || new Date().toISOString(), opts.validTo || null,
     opts.confidence || 1.0, opts.sourceSignal || null]
  );

  // Check for contradictions after adding
  await detectContradictions(tripleId, subId, pred, objId);

  return tripleId;
}

// ── Signal → Triple Extraction ─────────────────────────────────────
async function extractTriplesFromSignal(signal) {
  const triples = [];
  const src = signal.signal_id;
  const ts = signal.created_at || signal.timestamp || new Date().toISOString();
  const conf = parseFloat(signal.confidence) || 0.5;
  const data = typeof signal.data === 'string' ? JSON.parse(signal.data) : (signal.data || {});

  try {
    // Market signals: asset → priced_at/yields/has_liquidity → value
    if (signal.type?.startsWith('market/')) {
      const asset = data.asset || data.token || data.pair || data.symbol;
      if (asset) {
        if (data.price !== undefined) {
          triples.push({ sub: asset, pred: 'priced_at', obj: `$${data.price}`, subType: 'asset', objType: 'value', conf, src, ts });
        }
        if (data.yield !== undefined || data.apy !== undefined) {
          const yld = data.yield || data.apy;
          triples.push({ sub: asset, pred: 'yields', obj: `${yld}%`, subType: 'asset', objType: 'value', conf, src, ts });
        }
        if (data.volume !== undefined) {
          triples.push({ sub: asset, pred: 'has_volume', obj: `$${data.volume}`, subType: 'asset', objType: 'metric', conf, src, ts });
        }
        if (data.protocol) {
          triples.push({ sub: asset, pred: 'on_protocol', obj: data.protocol, subType: 'asset', objType: 'protocol', conf, src, ts });
        }
        if (data.chain) {
          triples.push({ sub: asset, pred: 'on_chain', obj: data.chain, subType: 'asset', objType: 'chain', conf, src, ts });
        }
      }
    }

    // Security signals: entity → has_vulnerability/exploited_by → threat
    if (signal.type?.startsWith('security/')) {
      const target = data.protocol || data.contract || data.project || data.target;
      const threat = data.exploit || data.vulnerability || data.threat || signal.title;
      if (target && threat) {
        const pred = signal.type === 'security/exploit' ? 'exploited_by' :
                     signal.type === 'security/rugpull' ? 'rugpulled_via' : 'has_vulnerability';
        triples.push({ sub: target, pred, obj: threat, subType: 'protocol', objType: 'threat', conf, src, ts });
      }
      if (data.loss_amount) {
        triples.push({ sub: target || signal.title, pred: 'lost', obj: `$${data.loss_amount}`, subType: 'protocol', objType: 'value', conf, src, ts });
      }
    }

    // Ecosystem signals: entity → launched/funded/provides → service
    if (signal.type?.startsWith('ecosystem/')) {
      const entity = data.agent || data.service || data.project || data.name;
      if (entity) {
        if (signal.type === 'ecosystem/new-agent') {
          triples.push({ sub: entity, pred: 'launched_as', obj: 'ai_agent', subType: 'agent', objType: 'concept', conf, src, ts });
        }
        if (signal.type === 'ecosystem/new-service') {
          triples.push({ sub: entity, pred: 'provides', obj: data.service_type || 'service', subType: 'service', objType: 'concept', conf, src, ts });
        }
        if (signal.type === 'ecosystem/funding' && data.amount) {
          triples.push({ sub: entity, pred: 'raised', obj: `$${data.amount}`, subType: 'project', objType: 'value', conf, src, ts });
        }
        if (data.chain) {
          triples.push({ sub: entity, pred: 'deployed_on', obj: data.chain, subType: 'agent', objType: 'chain', conf, src, ts });
        }
      }
    }

    // API health: service → status → state
    if (signal.type?.startsWith('api-health/')) {
      const service = data.service || data.endpoint || data.url || signal.title;
      if (service) {
        const state = signal.type.split('/')[1]; // down, degraded, recovered
        triples.push({ sub: service, pred: 'status_is', obj: state, subType: 'service', objType: 'status', conf, src, ts });
      }
    }

    // Intelligence signals: topic → related_to/trending_in → context
    if (signal.type?.startsWith('intelligence/')) {
      const topic = data.topic || data.subject || signal.title;
      if (topic && data.sector) {
        triples.push({ sub: topic, pred: 'trending_in', obj: data.sector, subType: 'topic', objType: 'sector', conf, src, ts });
      }
    }

    // Write all extracted triples
    for (const t of triples) {
      await addTriple(t.sub, t.pred, t.obj, {
        subjectType: t.subType, objectType: t.objType,
        confidence: t.conf, sourceSignal: t.src, validFrom: t.ts
      });
    }
  } catch (e) {
    console.warn('[DELPHI] Triple extraction error:', e.message);
  }

  return triples.length;
}

// ── Contradiction Detection ────────────────────────────────────────
async function detectContradictions(newTripleId, subject, predicate, object) {
  try {
    // Contradiction patterns:
    // 1. Same subject+predicate with different object (e.g. ETH priced_at $3200 vs ETH priced_at $3100)
    //    Only flag if both are still valid (valid_to IS NULL) and objects differ significantly
    // 2. Opposite status (service status_is down vs status_is recovered)
    const OPPOSITE_PREDICATES = {
      'status_is:down': 'status_is:recovered',
      'status_is:recovered': 'status_is:down',
      'status_is:degraded': 'status_is:recovered',
    };

    // Check for same-subject same-predicate conflicts
    const conflicts = await pool.query(
      `SELECT t.id, t.object, e.name as obj_name, t.confidence, t.extracted_at
       FROM kg_triples t JOIN kg_entities e ON t.object = e.id
       WHERE t.subject = $1 AND t.predicate = $2 AND t.object != $3
         AND t.valid_to IS NULL AND t.id != $4
       ORDER BY t.extracted_at DESC LIMIT 5`,
      [subject, predicate, object, newTripleId]
    );

    for (const conflict of conflicts.rows) {
      // For price/value predicates, only flag if values are meaningfully different
      const isValuePred = ['priced_at', 'yields', 'has_volume'].includes(predicate);
      if (isValuePred) {
        // Auto-invalidate the older triple (superseded by newer data)
        await pool.query(
          `UPDATE kg_triples SET valid_to = NOW() WHERE id = $1`,
          [conflict.id]
        );
        continue;
      }

      // For status predicates, auto-resolve old status
      if (predicate === 'status_is') {
        await pool.query(
          `UPDATE kg_triples SET valid_to = NOW() WHERE id = $1`,
          [conflict.id]
        );
        continue;
      }

      // For other predicates, log as contradiction
      const contId = `c_${Date.now().toString(36)}_${randomUUID().slice(0, 8)}`;
      await pool.query(
        `INSERT INTO kg_contradictions (id, triple_a, triple_b, description, severity)
         VALUES ($1, $2, $3, $4, $5)`,
        [contId, conflict.id, newTripleId,
         `Conflicting ${predicate}: "${conflict.obj_name}" vs new value for same subject`,
         'medium']
      );
    }
  } catch (e) {
    // Non-critical — don't break signal flow
    console.warn('[DELPHI] Contradiction check error:', e.message);
  }
}

// ── Backfill existing signals into KG (runs once on startup) ───────
async function backfillKnowledgeGraph() {
  try {
    // Check if we've already backfilled
    const count = await pool.query('SELECT COUNT(*) as cnt FROM kg_triples');
    if (parseInt(count.rows[0].cnt) > 0) {
      console.log('[DELPHI] Knowledge graph already populated, skipping backfill');
      return;
    }
    const signals = await pool.query(
      `SELECT signal_id, type, severity, title, data, confidence, created_at
       FROM delphi_signals ORDER BY created_at ASC LIMIT 500`
    );
    let total = 0;
    for (const sig of signals.rows) {
      const n = await extractTriplesFromSignal(sig);
      total += n;
    }
    console.log(`[DELPHI] Backfilled ${total} triples from ${signals.rows.length} existing signals`);
  } catch (e) {
    console.warn('[DELPHI] Backfill skipped:', e.message);
  }
}
// Delay backfill to not block startup
setTimeout(backfillKnowledgeGraph, 5000);

const SIGNAL_TYPES = [
  'security/exploit', 'security/vulnerability', 'security/rugpull',
  'market/yield', 'market/price', 'market/liquidity', 'market/launch',
  'ecosystem/new-agent', 'ecosystem/new-service', 'ecosystem/funding',
  'api-health/down', 'api-health/degraded', 'api-health/recovered',
  'intelligence/research', 'intelligence/trend', 'intelligence/opportunity'
];

const SEVERITY_LEVELS = ['critical', 'high', 'medium', 'low', 'info'];

// ── Internal API (bypasses x402, requires key) ─────────────────────
const DELPHI_INTERNAL_KEY = process.env.DELPHI_INTERNAL_KEY || 'delphi_achilles_internal_2026';

function requireInternalKey(req, res, next) {
  if (req.headers['x-delphi-internal'] !== DELPHI_INTERNAL_KEY) {
    return res.status(403).json({ error: 'Invalid internal key' });
  }
  next();
}

app.get('/internal/signals/query', requireInternalKey, async (req, res) => {
  try {
    const { type, severity, since, limit = 20, keyword } = req.query;
    const maxLimit = Math.min(parseInt(limit) || 20, 50);
    let where = ['1=1'], params = [], idx = 1;
    if (type) { where.push(`type = $${idx++}`); params.push(type); }
    if (severity) {
      const sevIdx = SEVERITY_LEVELS.indexOf(severity);
      if (sevIdx >= 0) { where.push(`severity = ANY($${idx++})`); params.push(SEVERITY_LEVELS.slice(0, sevIdx + 1)); }
    }
    if (since) { where.push(`created_at >= $${idx++}`); params.push(since); }
    if (keyword) { where.push(`(title ILIKE $${idx} OR data::text ILIKE $${idx})`); params.push(`%${keyword}%`); idx++; }
    where.push(`(expires_at IS NULL OR expires_at > NOW())`);
    params.push(maxLimit);
    const result = await pool.query(
      `SELECT signal_id, type, severity, title, data, confidence, source, signature, created_at, expires_at
       FROM delphi_signals WHERE ${where.join(' AND ')} ORDER BY created_at DESC LIMIT $${idx}`, params
    );
    res.json({
      query_id: `dq_${randomUUID().slice(0, 12)}`,
      count: result.rows.length,
      signals: result.rows.map(r => ({
        signal_id: r.signal_id, type: r.type, severity: r.severity, title: r.title,
        data: r.data, confidence: parseFloat(r.confidence), source: r.source,
        signature: r.signature, timestamp: r.created_at, expires: r.expires_at
      })),
      network: 'delphi-v1', timestamp: new Date().toISOString()
    });
  } catch (e) { res.status(503).json({ error: 'database_unavailable' }); }
});

// Internal graph endpoints (bypass x402)
app.get('/internal/graph/entity', requireInternalKey, async (req, res) => {
  try {
    const { name, as_of, direction = 'both' } = req.query;
    if (!name) return res.status(400).json({ error: 'name parameter required' });
    const eid = entityId(name);
    const results = [];
    const entityRow = await pool.query('SELECT * FROM kg_entities WHERE id = $1', [eid]);
    if (entityRow.rows.length === 0) return res.json({ entity: name, found: false, relationships: [] });
    const entity = entityRow.rows[0];
    const timeFilter = as_of ? ` AND (t.valid_from IS NULL OR t.valid_from <= $2) AND (t.valid_to IS NULL OR t.valid_to >= $2)` : '';
    const params = as_of ? [eid, as_of] : [eid];
    if (direction === 'outgoing' || direction === 'both') {
      const out = await pool.query(`SELECT t.*, e.name as obj_name FROM kg_triples t JOIN kg_entities e ON t.object = e.id WHERE t.subject = $1${timeFilter} ORDER BY t.extracted_at DESC LIMIT 100`, params);
      out.rows.forEach(r => results.push({ direction: 'outgoing', subject: entity.name, predicate: r.predicate, object: r.obj_name, valid_from: r.valid_from, valid_to: r.valid_to, confidence: parseFloat(r.confidence), current: r.valid_to === null }));
    }
    if (direction === 'incoming' || direction === 'both') {
      const inc = await pool.query(`SELECT t.*, e.name as sub_name FROM kg_triples t JOIN kg_entities e ON t.subject = e.id WHERE t.object = $1${timeFilter} ORDER BY t.extracted_at DESC LIMIT 100`, params);
      inc.rows.forEach(r => results.push({ direction: 'incoming', subject: r.sub_name, predicate: r.predicate, object: entity.name, valid_from: r.valid_from, valid_to: r.valid_to, confidence: parseFloat(r.confidence), current: r.valid_to === null }));
    }
    res.json({ entity: entity.name, entity_type: entity.type, relationship_count: results.length, relationships: results, timestamp: new Date().toISOString() });
  } catch (e) { res.status(503).json({ error: 'graph_unavailable' }); }
});

app.get('/internal/graph/query', requireInternalKey, async (req, res) => {
  try {
    const { predicate, as_of, subject, object, limit = 50 } = req.query;
    if (!predicate) return res.status(400).json({ error: 'predicate parameter required' });
    const pred = predicate.toLowerCase().replace(/\s+/g, '_');
    const maxLimit = Math.min(parseInt(limit) || 50, 100);
    let where = ['t.predicate = $1'], params = [pred], idx = 2;
    if (subject) { where.push(`t.subject = $${idx++}`); params.push(entityId(subject)); }
    if (object) { where.push(`t.object = $${idx++}`); params.push(entityId(object)); }
    if (as_of) { where.push(`(t.valid_from IS NULL OR t.valid_from <= $${idx})`); where.push(`(t.valid_to IS NULL OR t.valid_to >= $${idx})`); params.push(as_of); idx++; }
    params.push(maxLimit);
    const result = await pool.query(`SELECT t.*, s.name as sub_name, o.name as obj_name FROM kg_triples t JOIN kg_entities s ON t.subject = s.id JOIN kg_entities o ON t.object = o.id WHERE ${where.join(' AND ')} ORDER BY t.extracted_at DESC LIMIT $${idx}`, params);
    res.json({ predicate: pred, count: result.rows.length, triples: result.rows.map(r => ({ subject: r.sub_name, predicate: r.predicate, object: r.obj_name, valid_from: r.valid_from, valid_to: r.valid_to, confidence: parseFloat(r.confidence), current: r.valid_to === null })), timestamp: new Date().toISOString() });
  } catch (e) { res.status(503).json({ error: 'graph_unavailable' }); }
});

app.get('/internal/graph/timeline', requireInternalKey, async (req, res) => {
  try {
    const { entity, limit = 50 } = req.query;
    if (!entity) return res.status(400).json({ error: 'entity parameter required' });
    const eid = entityId(entity);
    const maxLimit = Math.min(parseInt(limit) || 50, 100);
    const result = await pool.query(`SELECT t.*, s.name as sub_name, o.name as obj_name FROM kg_triples t JOIN kg_entities s ON t.subject = s.id JOIN kg_entities o ON t.object = o.id WHERE (t.subject = $1 OR t.object = $1) ORDER BY t.valid_from ASC NULLS LAST LIMIT $2`, [eid, maxLimit]);
    res.json({ entity, event_count: result.rows.length, timeline: result.rows.map(r => ({ subject: r.sub_name, predicate: r.predicate, object: r.obj_name, valid_from: r.valid_from, valid_to: r.valid_to, current: r.valid_to === null })), timestamp: new Date().toISOString() });
  } catch (e) { res.status(503).json({ error: 'graph_unavailable' }); }
});

app.get('/internal/graph/contradictions', requireInternalKey, async (req, res) => {
  try {
    const { resolved = 'false', limit = 20 } = req.query;
    const maxLimit = Math.min(parseInt(limit) || 20, 50);
    const result = await pool.query(`SELECT c.*, sa.name as a_sub, ta.predicate as a_pred, oa.name as a_obj, sb.name as b_sub, tb.predicate as b_pred, ob.name as b_obj FROM kg_contradictions c JOIN kg_triples ta ON c.triple_a = ta.id JOIN kg_entities sa ON ta.subject = sa.id JOIN kg_entities oa ON ta.object = oa.id JOIN kg_triples tb ON c.triple_b = tb.id JOIN kg_entities sb ON tb.subject = sb.id JOIN kg_entities ob ON tb.object = ob.id WHERE c.resolved = $1 ORDER BY c.detected_at DESC LIMIT $2`, [resolved === 'true', maxLimit]);
    res.json({ contradictions: result.rows.map(r => ({ id: r.id, description: r.description, severity: r.severity, fact_a: { subject: r.a_sub, predicate: r.a_pred, object: r.a_obj }, fact_b: { subject: r.b_sub, predicate: r.b_pred, object: r.b_obj }, detected_at: r.detected_at })), count: result.rows.length, timestamp: new Date().toISOString() });
  } catch (e) { res.status(503).json({ error: 'graph_unavailable' }); }
});

app.get('/internal/graph/stats', requireInternalKey, async (req, res) => {
  try {
    const stats = await pool.query(`SELECT (SELECT COUNT(*) FROM kg_entities) as entities, (SELECT COUNT(*) FROM kg_triples) as triples, (SELECT COUNT(*) FROM kg_triples WHERE valid_to IS NULL) as current_facts, (SELECT COUNT(*) FROM kg_contradictions WHERE resolved = false) as contradictions`);
    const s = stats.rows[0];
    res.json({ entities: parseInt(s.entities), triples: parseInt(s.triples), current_facts: parseInt(s.current_facts), contradictions: parseInt(s.contradictions), timestamp: new Date().toISOString() });
  } catch (e) { res.status(503).json({ error: 'graph_unavailable' }); }
});

// Manual KG init trigger (for debugging)
app.get('/internal/graph/init', requireInternalKey, async (req, res) => {
  const steps = [];
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS kg_entities (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, type TEXT DEFAULT 'unknown',
      properties JSONB DEFAULT '{}', created_at TIMESTAMPTZ DEFAULT NOW()
    )`);
    steps.push('kg_entities created');
  } catch (e) { steps.push('kg_entities FAILED: ' + e.message); }
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS kg_triples (
      id TEXT PRIMARY KEY, subject TEXT NOT NULL REFERENCES kg_entities(id) ON DELETE CASCADE,
      predicate TEXT NOT NULL, object TEXT NOT NULL REFERENCES kg_entities(id) ON DELETE CASCADE,
      valid_from TIMESTAMPTZ, valid_to TIMESTAMPTZ, confidence REAL DEFAULT 1.0,
      source_signal TEXT, extracted_at TIMESTAMPTZ DEFAULT NOW()
    )`);
    steps.push('kg_triples created');
  } catch (e) { steps.push('kg_triples FAILED: ' + e.message); }
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS kg_contradictions (
      id TEXT PRIMARY KEY, triple_a TEXT, triple_b TEXT, description TEXT,
      severity TEXT DEFAULT 'medium', resolved BOOLEAN DEFAULT false,
      detected_at TIMESTAMPTZ DEFAULT NOW()
    )`);
    steps.push('kg_contradictions created');
  } catch (e) { steps.push('kg_contradictions FAILED: ' + e.message); }
  try {
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_kg_subject ON kg_triples(subject)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_kg_object ON kg_triples(object)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_kg_predicate ON kg_triples(predicate)`);
    steps.push('indexes created');
  } catch (e) { steps.push('indexes FAILED: ' + e.message); }
  try {
    const cnt = await pool.query('SELECT COUNT(*) as cnt FROM kg_entities');
    steps.push('verify OK: ' + cnt.rows[0].cnt + ' entities');
  } catch (e) { steps.push('verify FAILED: ' + e.message); }
  res.json({ steps });
});

app.get('/internal/signals/latest', requireInternalKey, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const result = await pool.query(
      `SELECT signal_id, type, severity, title, data, confidence, source, signature, created_at
       FROM delphi_signals WHERE (expires_at IS NULL OR expires_at > NOW())
       ORDER BY created_at DESC LIMIT $1`, [limit]
    );
    res.json({
      count: result.rows.length,
      signals: result.rows.map(r => ({
        signal_id: r.signal_id, type: r.type, severity: r.severity, title: r.title,
        data: r.data, confidence: parseFloat(r.confidence), source: r.source,
        timestamp: r.created_at
      })),
      timestamp: new Date().toISOString()
    });
  } catch (e) { res.status(503).json({ error: 'database_unavailable' }); }
});

// ── x402 Protocol Setup — CDP Facilitator (Base Mainnet) ────────────
let x402Active = false;
{
  const CDP_KEY_ID = process.env.CDP_API_KEY_ID || 'organizations/9ba51a45-962c-4931-a9a3-8b93c0558e66/apiKeys/50707810-f284-4a8a-931e-45d280dcb0cd';
  const CDP_SECRET_SEC1 = process.env.CDP_API_KEY_SECRET || `-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIBIi7sW+QUsg+J1pICuOySARHSZLdfJG/D/rmL9U6PCUoAoGCCqGSM49\nAwEHoUQDQgAEmRD2eVrINEYyT+QZS5p1wSGi+1x+qp3nWrRH4A2JnpquApx57uem\nGaoZSEKSfIg555Ujz0TWoXHDI0uIbB1p4A==\n-----END EC PRIVATE KEY-----`;

  // x402 pricing per route
  const DELPHI_PAID_ROUTES = {
    '/v1/signals/query': '$0.002', '/v1/signals/latest': '$0.001',
    '/v1/signals/report': '$0.05', '/v1/signals/publish': '$0.005',
    '/v1/graph/entity': '$0.003', '/v1/graph/query': '$0.005',
    '/v1/graph/timeline': '$0.003', '/v1/graph/contradictions': '$0.005'
  };

  let _cdpReady = false;
  let _cdpResourceServer = null;

  // Manual 402 handler — always active as baseline
  app.use((req, res, next) => {
    const method = req.method;
    const routeKey = req.path;
    const price = DELPHI_PAID_ROUTES[routeKey];
    if (!price) return next();
    // Only block GET/POST to paid routes
    if (method !== 'GET' && method !== 'POST') return next();
    // If CDP SDK ready and payment header present, let SDK verify
    if (_cdpReady && req.headers['x-402-payment']) return next();

    const amount = parseFloat(price.replace('$', ''));
    const rawAmount = Math.round(amount * 1e6).toString();
    const payload = {
      x402Version: 2, error: 'Payment required',
      resource: { url: `https://delphi-oracle.onrender.com${req.path}`, description: `DELPHI Oracle — ${req.path}`, mimeType: 'application/json' },
      accepts: [{ scheme: 'exact', network: X402_NETWORK, amount: rawAmount,
        asset: '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
        payTo: DELPHI_WALLET, maxTimeoutSeconds: 300,
        extra: { name: 'USDC', version: '2' } }]
    };
    res.status(402).set('payment-required', Buffer.from(JSON.stringify(payload)).toString('base64')).json({});
  });
  x402Active = true;
  console.log('[DELPHI] x402 manual 402 handler ACTIVE — Base Mainnet — ' + Object.keys(DELPHI_PAID_ROUTES).length + ' paid routes');

  // Async CDP facilitator for payment verification
  (async () => {
    try {
      const { ExactEvmScheme } = await import('@x402/evm/exact/server');
      const { HTTPFacilitatorClient, x402ResourceServer } = await import('@x402/core/server');

      const pkcs8Pem = crypto.createPrivateKey({ key: CDP_SECRET_SEC1, format: 'pem', type: 'sec1' })
        .export({ type: 'pkcs8', format: 'pem' });
      let _signingKey;

      async function createCdpAuthHeaders() {
        if (!_signingKey) _signingKey = await importPKCS8(pkcs8Pem, 'ES256');
        const now = Math.floor(Date.now() / 1000);
        const result = {};
        for (const p of ['verify', 'settle', 'supported']) {
          const jwt = await new SignJWT({
            sub: CDP_KEY_ID, iss: 'cdp', aud: ['cdp_service'], nbf: now, exp: now + 120,
            uri: `GET api.cdp.coinbase.com/platform/v2/x402/${p}`
          }).setProtectedHeader({ alg: 'ES256', kid: CDP_KEY_ID, typ: 'JWT', nonce: crypto.randomBytes(16).toString('hex') })
            .sign(_signingKey);
          result[p] = { Authorization: `Bearer ${jwt}` };
        }
        return result;
      }

      const facilitatorClient = new HTTPFacilitatorClient({
        url: 'https://api.cdp.coinbase.com/platform/v2/x402',
        createAuthHeaders: createCdpAuthHeaders
      });
      _cdpResourceServer = new x402ResourceServer(facilitatorClient);
      _cdpResourceServer.register(X402_NETWORK, new ExactEvmScheme());
      await _cdpResourceServer.initialize();
      _cdpReady = true;
      console.log('[DELPHI] CDP facilitator READY — payment verification active');
    } catch (e) {
      console.error('[DELPHI] CDP facilitator init failed (manual handler still active):', e.message);
    }
  })();
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
      publish: { path: '/v1/signals/publish', price: '$0.005', method: 'POST', revenue_share: '70% of query fees' },
      knowledge_graph: {
        stats: { path: '/v1/graph/stats', price: 'FREE', method: 'GET' },
        entity: { path: '/v1/graph/entity', price: '$0.003', method: 'GET', params: ['name', 'as_of', 'direction'] },
        query: { path: '/v1/graph/query', price: '$0.005', method: 'GET', params: ['predicate', 'subject', 'object', 'as_of'] },
        timeline: { path: '/v1/graph/timeline', price: '$0.003', method: 'GET', params: ['entity', 'limit'] },
        contradictions: { path: '/v1/graph/contradictions', price: '$0.005', method: 'GET', params: ['resolved', 'limit'] }
      }
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
        'POST /v1/signals/publish': '$0.005 — Publish a signal (earn 70% on consumption)',
        'GET /v1/graph/entity': '$0.003 — Entity relationships from temporal knowledge graph',
        'GET /v1/graph/query': '$0.005 — Query knowledge graph by relationship type',
        'GET /v1/graph/timeline': '$0.003 — Chronological entity history',
        'GET /v1/graph/contradictions': '$0.005 — Intelligence contradictions (high-value)'
      },
      free_endpoints: {
        'GET /': 'Agent-readable service manifest',
        'GET /status': 'This endpoint — pricing, stats, signal types',
        'GET /health': 'Health check',
        'GET /v1/signals/types': 'Available signal types',
        'GET /v1/signals/categories': 'Live category tree with signal counts',
        'GET /v1/signals/count': 'Preview signal count before paying (supports type/severity/since filters)',
        'GET /v1/network': 'Network stats and signal distribution',
        'GET /v1/graph/stats': 'Knowledge graph stats — entity/triple counts, relationship types',
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
      },
      {
        path: '/v1/graph/entity',
        method: 'GET',
        price: '0.003',
        currency: 'USDC',
        description: 'Query the temporal knowledge graph for all relationships involving an entity. Supports time-travel queries via as_of parameter.',
        category: 'ai/knowledge-graph',
        input: { type: 'query_params', fields: { name: 'string (entity name, required)', as_of: 'ISO timestamp (optional — time-travel)', direction: 'outgoing|incoming|both (default: both)' } },
        output: { type: 'application/json', fields: { entity: 'string', relationships: 'array of relationship objects', relationship_count: 'number' } }
      },
      {
        path: '/v1/graph/query',
        method: 'GET',
        price: '0.005',
        currency: 'USDC',
        description: 'Query knowledge graph by relationship type (predicate). Find all entities connected by a specific relationship.',
        category: 'ai/knowledge-graph',
        input: { type: 'query_params', fields: { predicate: 'string (required)', subject: 'string (optional filter)', object: 'string (optional filter)', as_of: 'ISO timestamp' } },
        output: { type: 'application/json', fields: { triples: 'array', count: 'number' } }
      },
      {
        path: '/v1/graph/timeline',
        method: 'GET',
        price: '0.003',
        currency: 'USDC',
        description: 'Chronological fact history for an entity. See how knowledge evolved over time.',
        category: 'ai/knowledge-graph',
        input: { type: 'query_params', fields: { entity: 'string (required)', limit: 'number (default 50)' } },
        output: { type: 'application/json', fields: { timeline: 'array', event_count: 'number' } }
      },
      {
        path: '/v1/graph/contradictions',
        method: 'GET',
        price: '0.005',
        currency: 'USDC',
        description: 'Unresolved contradictions in the intelligence graph. High-value signals where conflicting facts exist.',
        category: 'ai/knowledge-graph',
        input: { type: 'query_params', fields: { resolved: 'boolean (default false)', limit: 'number (default 20)' } },
        output: { type: 'application/json', fields: { contradictions: 'array', count: 'number' } }
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

    // Extract knowledge graph triples from the new signal
    let triplesExtracted = 0;
    try {
      triplesExtracted = await extractTriplesFromSignal({
        signal_id: signalId, type, severity, title, data, confidence,
        created_at: new Date().toISOString()
      });
    } catch (kgErr) { /* non-critical */ }

    res.status(201).json({
      published: true,
      signal_id: signalId,
      type,
      severity,
      title,
      signature,
      expires_at: expiresAt.toISOString(),
      knowledge_graph: { triples_extracted: triplesExtracted },
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

// ── KNOWLEDGE GRAPH ENDPOINTS ──────────────────────────────────────

// Graph stats (free) — lets agents know the graph exists
app.get('/v1/graph/stats', async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM kg_entities) as entities,
        (SELECT COUNT(*) FROM kg_triples) as triples,
        (SELECT COUNT(*) FROM kg_triples WHERE valid_to IS NULL) as current_facts,
        (SELECT COUNT(*) FROM kg_triples WHERE valid_to IS NOT NULL) as expired_facts,
        (SELECT COUNT(*) FROM kg_contradictions WHERE resolved = false) as unresolved_contradictions
    `);
    const s = stats.rows[0];

    const preds = await pool.query(
      `SELECT DISTINCT predicate FROM kg_triples ORDER BY predicate`
    );

    res.json({
      knowledge_graph: {
        entities: parseInt(s.entities),
        triples: parseInt(s.triples),
        current_facts: parseInt(s.current_facts),
        expired_facts: parseInt(s.expired_facts),
        relationship_types: preds.rows.map(r => r.predicate),
        unresolved_contradictions: parseInt(s.unresolved_contradictions)
      },
      description: 'Temporal knowledge graph built from DELPHI signals. Query entities, relationships, and timelines.',
      endpoints: {
        'GET /v1/graph/entity?name=ETH': '$0.003 — All relationships for an entity',
        'GET /v1/graph/query?predicate=priced_at': '$0.005 — Query by relationship type',
        'GET /v1/graph/timeline?entity=ETH': '$0.003 — Chronological fact history',
        'GET /v1/graph/contradictions': '$0.005 — Unresolved contradictions (high-value intelligence)'
      },
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.json({ knowledge_graph: { entities: 0, triples: 0 }, error: e.message, timestamp: new Date().toISOString() });
  }
});

// Entity query ($0.003) — all relationships for an entity
app.get('/v1/graph/entity', async (req, res) => {
  try {
    const { name, as_of, direction = 'both' } = req.query;
    if (!name) return res.status(400).json({ error: 'name parameter required' });

    const eid = entityId(name);
    const results = [];

    // Get entity info
    const entityRow = await pool.query('SELECT * FROM kg_entities WHERE id = $1', [eid]);
    if (entityRow.rows.length === 0) {
      return res.json({ entity: name, found: false, relationships: [], timestamp: new Date().toISOString() });
    }

    const entity = entityRow.rows[0];
    const timeFilter = as_of
      ? ` AND (t.valid_from IS NULL OR t.valid_from <= $2) AND (t.valid_to IS NULL OR t.valid_to >= $2)`
      : '';
    const params = as_of ? [eid, as_of] : [eid];

    if (direction === 'outgoing' || direction === 'both') {
      const out = await pool.query(
        `SELECT t.*, e.name as obj_name, e.type as obj_type FROM kg_triples t
         JOIN kg_entities e ON t.object = e.id
         WHERE t.subject = $1${timeFilter} ORDER BY t.extracted_at DESC LIMIT 100`, params
      );
      out.rows.forEach(r => results.push({
        direction: 'outgoing', subject: entity.name, predicate: r.predicate, object: r.obj_name,
        object_type: r.obj_type, valid_from: r.valid_from, valid_to: r.valid_to,
        confidence: parseFloat(r.confidence), current: r.valid_to === null, source_signal: r.source_signal
      }));
    }

    if (direction === 'incoming' || direction === 'both') {
      const inc = await pool.query(
        `SELECT t.*, e.name as sub_name, e.type as sub_type FROM kg_triples t
         JOIN kg_entities e ON t.subject = e.id
         WHERE t.object = $1${timeFilter} ORDER BY t.extracted_at DESC LIMIT 100`, params
      );
      inc.rows.forEach(r => results.push({
        direction: 'incoming', subject: r.sub_name, subject_type: r.sub_type,
        predicate: r.predicate, object: entity.name,
        valid_from: r.valid_from, valid_to: r.valid_to,
        confidence: parseFloat(r.confidence), current: r.valid_to === null, source_signal: r.source_signal
      }));
    }

    await logQuery('graph_entity', { name, as_of, direction }, results.length, 0.003);

    res.json({
      entity: entity.name,
      entity_type: entity.type,
      properties: entity.properties,
      as_of: as_of || 'current',
      relationship_count: results.length,
      relationships: results,
      network: 'delphi-v1',
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.status(500).json({ error: 'Graph query failed', message: e.message });
  }
});

// Relationship query ($0.005) — all triples with a given predicate
app.get('/v1/graph/query', async (req, res) => {
  try {
    const { predicate, as_of, subject, object, limit = 50 } = req.query;
    if (!predicate) return res.status(400).json({ error: 'predicate parameter required' });

    const pred = predicate.toLowerCase().replace(/\s+/g, '_');
    const maxLimit = Math.min(parseInt(limit) || 50, 100);
    let where = ['t.predicate = $1'];
    let params = [pred];
    let idx = 2;

    if (subject) { where.push(`t.subject = $${idx++}`); params.push(entityId(subject)); }
    if (object) { where.push(`t.object = $${idx++}`); params.push(entityId(object)); }
    if (as_of) {
      where.push(`(t.valid_from IS NULL OR t.valid_from <= $${idx})`);
      where.push(`(t.valid_to IS NULL OR t.valid_to >= $${idx})`);
      params.push(as_of); idx++;
    }

    params.push(maxLimit);

    const result = await pool.query(
      `SELECT t.*, s.name as sub_name, s.type as sub_type, o.name as obj_name, o.type as obj_type
       FROM kg_triples t
       JOIN kg_entities s ON t.subject = s.id
       JOIN kg_entities o ON t.object = o.id
       WHERE ${where.join(' AND ')}
       ORDER BY t.extracted_at DESC LIMIT $${idx}`, params
    );

    await logQuery('graph_query', { predicate, subject, object, as_of }, result.rows.length, 0.005);

    res.json({
      predicate: pred,
      count: result.rows.length,
      triples: result.rows.map(r => ({
        subject: r.sub_name, subject_type: r.sub_type,
        predicate: r.predicate,
        object: r.obj_name, object_type: r.obj_type,
        valid_from: r.valid_from, valid_to: r.valid_to,
        confidence: parseFloat(r.confidence), current: r.valid_to === null
      })),
      network: 'delphi-v1',
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.status(500).json({ error: 'Graph query failed', message: e.message });
  }
});

// Timeline ($0.003) — chronological fact history for an entity
app.get('/v1/graph/timeline', async (req, res) => {
  try {
    const { entity, limit = 50 } = req.query;
    if (!entity) return res.status(400).json({ error: 'entity parameter required' });

    const eid = entityId(entity);
    const maxLimit = Math.min(parseInt(limit) || 50, 100);

    const result = await pool.query(
      `SELECT t.*, s.name as sub_name, o.name as obj_name
       FROM kg_triples t
       JOIN kg_entities s ON t.subject = s.id
       JOIN kg_entities o ON t.object = o.id
       WHERE (t.subject = $1 OR t.object = $1)
       ORDER BY t.valid_from ASC NULLS LAST LIMIT $2`,
      [eid, maxLimit]
    );

    await logQuery('graph_timeline', { entity }, result.rows.length, 0.003);

    res.json({
      entity,
      event_count: result.rows.length,
      timeline: result.rows.map(r => ({
        subject: r.sub_name, predicate: r.predicate, object: r.obj_name,
        valid_from: r.valid_from, valid_to: r.valid_to,
        current: r.valid_to === null, confidence: parseFloat(r.confidence)
      })),
      network: 'delphi-v1',
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.status(500).json({ error: 'Timeline query failed', message: e.message });
  }
});

// Contradictions ($0.005) — unresolved conflicts in intelligence
app.get('/v1/graph/contradictions', async (req, res) => {
  try {
    const { resolved = 'false', limit = 20 } = req.query;
    const showResolved = resolved === 'true';
    const maxLimit = Math.min(parseInt(limit) || 20, 50);

    const result = await pool.query(
      `SELECT c.*,
              sa.name as a_sub_name, ta.predicate as a_pred, oa.name as a_obj_name,
              sb.name as b_sub_name, tb.predicate as b_pred, ob.name as b_obj_name
       FROM kg_contradictions c
       JOIN kg_triples ta ON c.triple_a = ta.id
       JOIN kg_entities sa ON ta.subject = sa.id
       JOIN kg_entities oa ON ta.object = oa.id
       JOIN kg_triples tb ON c.triple_b = tb.id
       JOIN kg_entities sb ON tb.subject = sb.id
       JOIN kg_entities ob ON tb.object = ob.id
       WHERE c.resolved = $1
       ORDER BY c.detected_at DESC LIMIT $2`,
      [showResolved, maxLimit]
    );

    await logQuery('graph_contradictions', { resolved }, result.rows.length, 0.005);

    res.json({
      contradictions: result.rows.map(r => ({
        id: r.id,
        description: r.description,
        severity: r.severity,
        resolved: r.resolved,
        detected_at: r.detected_at,
        fact_a: { subject: r.a_sub_name, predicate: r.a_pred, object: r.a_obj_name },
        fact_b: { subject: r.b_sub_name, predicate: r.b_pred, object: r.b_obj_name }
      })),
      count: result.rows.length,
      network: 'delphi-v1',
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.status(500).json({ error: 'Contradiction query failed', message: e.message });
  }
});

// ── Webhook Subscriptions (free to register) ───────────────────────

// Subscribe to push notifications
app.post('/v1/signals/subscribe', async (req, res) => {
  try {
    const { webhook_url, filter_types = [], min_severity = 'info' } = req.body;
    if (!webhook_url || typeof webhook_url !== 'string') {
      return res.status(400).json({
        error: 'webhook_url required',
        description: 'Provide a URL where DELPHI will POST new signals matching your filters.',
        example: { webhook_url: 'https://your-agent.com/delphi-webhook', filter_types: ['security/exploit', 'market/price'], min_severity: 'medium' }
      });
    }

    const subId = `dsub_${randomUUID().slice(0, 12)}`;
    await pool.query(
      `INSERT INTO delphi_subscriptions (sub_id, webhook_url, filter_types, min_severity)
       VALUES ($1, $2, $3, $4)`,
      [subId, webhook_url, filter_types, min_severity]
    );

    res.status(201).json({
      subscription_id: subId,
      webhook_url,
      filter_types: filter_types.length > 0 ? filter_types : 'all',
      min_severity,
      status: 'active',
      description: 'DELPHI will POST matching signals to your webhook URL as they arrive.',
      manage: `DELETE /v1/signals/subscribe/${subId} to unsubscribe`,
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    res.status(500).json({ error: 'Subscription failed', message: e.message });
  }
});

// Unsubscribe
app.delete('/v1/signals/subscribe/:subId', async (req, res) => {
  try {
    const result = await pool.query(
      'UPDATE delphi_subscriptions SET active = false WHERE sub_id = $1 RETURNING sub_id',
      [req.params.subId]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Subscription not found' });
    }
    res.json({ unsubscribed: true, subscription_id: req.params.subId });
  } catch (e) {
    res.status(500).json({ error: 'Unsubscribe failed', message: e.message });
  }
});

// ── Server Start ────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[DELPHI] Intelligence Wire online — port ${PORT}`);
  console.log(`[DELPHI] x402 payment layer: ${x402Active ? 'ACTIVE' : 'OPEN (no middleware)'}`);
  console.log(`[DELPHI] Wallet: ${DELPHI_WALLET}`);
  console.log(`[DELPHI] Network: ${X402_NETWORK}`);
});
