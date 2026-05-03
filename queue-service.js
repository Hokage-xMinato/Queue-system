/**
 * Turnstile Queue Service — deploy on Render as a Web Service
 *
 * Processes one Turnstile solve at a time.
 * All job management is internal; callers only get a jobId (opaque UUID).
 *
 * Endpoints (all require header: x-queue-secret = QUEUE_SECRET env var):
 *   POST /enqueue          → { jobId }   body: { ip: string }
 *   GET  /status/:jobId    → { status, position, eta, token? }
 *   POST /cancel/:jobId    → { ok }      (no-op: IP jobs are never cancelled mid-flight)
 *
 * IP deduplication rules:
 *   - One job per IP at a time. Same IP re-enqueueing while queued/processing
 *     gets the SAME jobId back (position is preserved, no new slot used).
 *   - Same IP re-enqueueing after 'done' gets the cached token immediately.
 *   - Jobs are NEVER removed from the queue on /cancel — solve always
 *     completes so HuggingFace compute is not wasted. Token stays cached.
 *
 * Environment variables:
 *   QUEUE_SECRET     — shared secret between this service and the Next.js API route
 *   CF_SOLVER_URL    — upstream Turnstile solver URL  (default: https://cf-rp12.onrender.com/cf-clearance-scraper)
 *   TURNSTILE_URL    — page URL passed to solver      (default: https://pw-olive-kappa.vercel.app/player)
 *   TURNSTILE_KEY    — Turnstile siteKey              (default: 0x4AAAAAACqytllG1rHL_Acz)
 *   PORT             — HTTP port                      (default: 3000)
 *   JOB_TTL_MS       — ms to keep completed jobs      (default: 120000 = 2 min)
 *   MAX_QUEUE_SIZE   — reject enqueue when queue full (default: 50)
 */

'use strict';

const http   = require('http');
const crypto = require('crypto');
const { HttpsProxyAgent } = require('https-proxy-agent');

// ── Config ────────────────────────────────────────────────────────────────────
const PORT           = parseInt(process.env.PORT           || '3000', 10);
const QUEUE_SECRET   = process.env.QUEUE_SECRET            || 'fucku123';
const CF_SOLVER_URL  = process.env.CF_SOLVER_URL           || 'https://yoursxminato-ai-cloud.hf.space/cf-clearance-scraper';
const TURNSTILE_URL  = process.env.TURNSTILE_URL           || 'https://studyspark.study/player';
const TURNSTILE_KEY  = process.env.TURNSTILE_KEY           || '0x4AAAAAACqytllG1rHL_Acz';
const JOB_TTL_MS     = parseInt(process.env.JOB_TTL_MS    || '120000', 10);
const MAX_QUEUE_SIZE = parseInt(process.env.MAX_QUEUE_SIZE || '50',     10);
const SOLVE_TIMEOUT  = 45_000; // ms — hard timeout per solve attempt

if (!QUEUE_SECRET) {
  console.error('[queue] FATAL: QUEUE_SECRET env var is not set');
  process.exit(1);
}

// ── Job store ─────────────────────────────────────────────────────────────────
// status: 'queued' | 'processing' | 'done' | 'error'
/** @type {Map<string, {id:string, status:string, ip:string, createdAt:number, finishedAt?:number, token?:string, errorCode?:string}>} */
const jobs = new Map();
/** @type {string[]} ordered queue of jobIds waiting to run */
const queue = [];
/** @type {Map<string, string>} ip → jobId — one active job per IP */
const ipIndex = new Map();

let isProcessing = false;

// ── Helpers ───────────────────────────────────────────────────────────────────
function newId() { return crypto.randomUUID(); }

function queuePosition(jobId) {
  const idx = queue.indexOf(jobId);
  return idx === -1 ? 0 : idx; // 0 = next up / already processing
}

/** Clean up old completed/errored jobs and their IP index entries */
function gc() {
  const cutoff = Date.now() - JOB_TTL_MS;
  for (const [id, job] of jobs) {
    if ((job.status === 'done' || job.status === 'error') && job.finishedAt < cutoff) {
      // Remove IP index only if it still points to this job
      if (ipIndex.get(job.ip) === id) ipIndex.delete(job.ip);
      jobs.delete(id);
    }
  }
}

// ── Turnstile solver ──────────────────────────────────────────────────────────
async function solveTurnstile() {
  const UA = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36';

  const PROXY_URL = 'http://zqxtpjjc-rotate:cknwbdszk5ux@p.webshare.io:80';
  const agent = new HttpsProxyAgent(PROXY_URL);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SOLVE_TIMEOUT);

  try {
    const res = await fetch(CF_SOLVER_URL, {
      method:  'POST',
      headers: {
        'Content-Type':    'application/json',
        'Accept':          '*/*',
        'Accept-Encoding': 'deflate, gzip',
        'Authorization':   'Bearer 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',
        'User-Agent':      UA,
      },
      body:   JSON.stringify({ url: TURNSTILE_URL, siteKey: TURNSTILE_KEY, mode: 'turnstile-min' }),
      signal: controller.signal,
      dispatcher: agent, // for Node 18+ native fetch use this
    });
    if (!res.ok) throw new Error(`solver_http_${res.status}`);
    const data = await res.json();
    if (!data.token) throw new Error('solver_no_token');
    return data.token;
  } finally {
    clearTimeout(timer);
  }
}
// ── Queue processor ────────────────────────────────────────────────────────────
async function processNext() {
  if (isProcessing || queue.length === 0) return;
  isProcessing = true;

  const jobId = queue[0]; // peek — remove only after finish
  const job   = jobs.get(jobId);

  if (!job) {
    // stale entry
    queue.shift();
    isProcessing = false;
    setImmediate(processNext);
    return;
  }

  job.status = 'processing';
  console.log(`[queue] processing job=${jobId}`);

  try {
    const token = await solveTurnstile();
    job.token      = token;
    job.status     = 'done';
    job.finishedAt = Date.now();
    console.log(`[queue] done job=${jobId}`);
  } catch (err) {
    job.status     = 'error';
    job.errorCode  = 'E_SOLVER';
    job.finishedAt = Date.now();
    console.error(`[queue] error job=${jobId}`, err.message);
  }

  queue.shift(); // now remove
  isProcessing = false;
  const TOKEN_TTL_MS = 5 * 60 * 1000; // 5 minutes

function gc() {
  const now = Date.now();
  for (const [id, job] of jobs) {
    if (job.status === 'done' || job.status === 'error') {
      const age = now - job.finishedAt;
      const tokenExpired = job.status === 'done' && age > TOKEN_TTL_MS;
      const jobExpired   = age > JOB_TTL_MS;

      if (tokenExpired || job.tokenUsed || jobExpired) {
        if (ipIndex.get(job.ip) === id) ipIndex.delete(job.ip);
        jobs.delete(id);
      }
    }
  }
}
  setImmediate(processNext); // process next without growing call stack
}

// ── HTTP server ───────────────────────────────────────────────────────────────
function readBody(req) {
  return new Promise((resolve, reject) => {
    let buf = '';
    req.on('data', c => { buf += c; if (buf.length > 4096) reject(new Error('body_too_large')); });
    req.on('end', () => {
      try { resolve(buf ? JSON.parse(buf) : {}); }
      catch { reject(new Error('invalid_json')); }
    });
    req.on('error', reject);
  });
}

function send(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) });
  res.end(payload);
}

const server = http.createServer(async (req, res) => {
  // ── Auth ──
  if (req.headers['x-queue-secret'] !== QUEUE_SECRET) {
    return send(res, 401, { error: 'unauthorized' });
  }

  const url    = new URL(req.url, `http://localhost`);
  const path   = url.pathname;
  const method = req.method;

  const statusMatch = path.match(/^\/status\/([0-9a-f-]{36})$/i);
  const cancelMatch = path.match(/^\/cancel\/([0-9a-f-]{36})$/i);

  // POST /enqueue
  if (method === 'POST' && path === '/enqueue') {
    let ip = '';
    try {
      const body = await readBody(req);
      ip = (body.ip || '').trim();
    } catch {
      return send(res, 400, { error: 'bad_request' });
    }
    if (!ip) return send(res, 400, { error: 'ip_required' });

    // ── IP dedup ──────────────────────────────────────────────────────────────
    const existingJobId = ipIndex.get(ip);
    if (existingJobId) {
      const existing = jobs.get(existingJobId);
      if (existing) {
        if (existing.status === 'queued' || existing.status === 'processing') {
          // Still in flight — return same jobId so client resumes its position
          console.log(`[queue] ip=${ip} rejoined existing job=${existingJobId} status=${existing.status}`);
          return send(res, 200, { jobId: existingJobId });
        }
        if (existing.status === 'done') {
          // Token already solved — return same jobId; client will read token via /status
          console.log(`[queue] ip=${ip} reusing cached token job=${existingJobId}`);
          return send(res, 200, { jobId: existingJobId });
        }
        // status === 'error' — fall through to create a fresh job
        if (ipIndex.get(ip) === existingJobId) ipIndex.delete(ip);
      } else {
        // Stale index entry
        ipIndex.delete(ip);
      }
    }

    if (queue.length >= MAX_QUEUE_SIZE) {
      return send(res, 429, { error: 'queue_full' });
    }

    const jobId = newId();
    jobs.set(jobId, { id: jobId, status: 'queued', ip, createdAt: Date.now() });
    ipIndex.set(ip, jobId);
    queue.push(jobId);
    console.log(`[queue] enqueued job=${jobId} ip=${ip} qlen=${queue.length}`);
    setImmediate(processNext);
    return send(res, 200, { jobId });
  }

  // GET /status/:jobId
  if (method === 'GET' && statusMatch) {
  const jobId = statusMatch[1];
  const job   = jobs.get(jobId);
  if (!job) return send(res, 404, { error: 'not_found' });

  const position = queuePosition(jobId);
  const eta      = position * 20;

  if (job.status === 'done') {
    // Check token age
    if (Date.now() - job.finishedAt > TOKEN_TTL_MS) {
      if (ipIndex.get(job.ip) === jobId) ipIndex.delete(job.ip);
      jobs.delete(jobId);
      return send(res, 200, { status: 'error', errorCode: 'E_TOKEN_EXPIRED' });
    }
    // Check single use
    if (job.tokenUsed) {
      return send(res, 200, { status: 'error', errorCode: 'E_TOKEN_USED' });
    }
    // Mark as used and return token
    job.tokenUsed = true;
    return send(res, 200, { status: 'done', token: job.token });
  }

  if (job.status === 'error') {
    return send(res, 200, { status: 'error', errorCode: job.errorCode });
  }

  return send(res, 200, { status: job.status, position, eta });
}

  // POST /cancel/:jobId
  // No-op by design: we never remove IP jobs from the queue mid-flight.
  // The solve always completes and the token is cached for when the IP reconnects.
  const cancelMatch = path.match(/^\/cancel\/([0-9a-f-]{36})$/i);
  if (method === 'POST' && cancelMatch) {
    return send(res, 200, { ok: true });
  }

  // Health check
  if (method === 'GET' && path === '/health') {
    return send(res, 200, { ok: true, queueLength: queue.length, isProcessing });
  }

  return send(res, 404, { error: 'not_found' });
});

server.listen(PORT, () => console.log(`[queue] listening on :${PORT}`));

// Graceful shutdown
process.on('SIGTERM', () => { server.close(() => process.exit(0)); });
