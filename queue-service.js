/**
 * Turnstile Queue Service — deploy on Render as a Web Service
 *
 * Processes one Turnstile solve at a time.
 * All job management is internal; callers only get a jobId (opaque UUID).
 */

'use strict';

const http   = require('http');
const crypto = require('crypto');
const { ProxyAgent } = require('undici');

// ── Config ────────────────────────────────────────────────────────────────────
const PORT           = parseInt(process.env.PORT           || '3000', 10);
const QUEUE_SECRET   = process.env.QUEUE_SECRET            || 'fucku123';
// Support multiple URLs separated by commas
const CF_SOLVER_URLS = (process.env.CF_SOLVER_URLS         || 'https://yoursxminato-huggingface.hf.space/cloudflare')
                         .split(',')
                         .map(url => url.trim())
                         .filter(url => url.length > 0);
                         
const TURNSTILE_URL  = process.env.TURNSTILE_URL           || 'https://studyspark.study/player';
const TURNSTILE_KEY  = process.env.TURNSTILE_KEY           || '0x4AAAAAACqytllG1rHL_Acz';
const JOB_TTL_MS     = parseInt(process.env.JOB_TTL_MS     || '120000', 10);
const MAX_QUEUE_SIZE = parseInt(process.env.MAX_QUEUE_SIZE || '50',     10);
const SOLVE_TIMEOUT  = 45_000; // ms — hard timeout per solve attempt
const TOKEN_TTL_MS   = parseInt(process.env.TOKEN_TTL_MS   || '300000', 10); // Default 5 mins

// OPTIONAL WAITING TIME BETWEEN JOBS (in milliseconds)
const JOB_DELAY_MS   = parseInt(process.env.JOB_DELAY_MS   || '5000', 10);

if (!QUEUE_SECRET) {
  console.error('[queue] FATAL: QUEUE_SECRET env var is not set');
  process.exit(1);
}

// ── Job store ─────────────────────────────────────────────────────────────────
const jobs = new Map();
const queue = [];
const ipIndex = new Map();

let isProcessing = false;
let currentSolverIndex = 0; // Tracks which solver URL to use next

// ── Helpers ───────────────────────────────────────────────────────────────────
function newId() { return crypto.randomUUID(); }

function queuePosition(jobId) {
  const idx = queue.indexOf(jobId);
  return idx === -1 ? 0 : idx; 
}

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

// Run GC periodically
setInterval(gc, 60000);

// ── Turnstile solver ──────────────────────────────────────────────────────────
async function solveTurnstile() {
  const UA = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36';

  const proxyAgent = new ProxyAgent({
    uri: 'http://p.webshare.io:80',
    token: `Basic ${Buffer.from('zqxtpjjc-rotate:cknwbdszk5ux').toString('base64')}`,
  });

  // Pick the current URL and advance the index (Round-Robin)
  const targetUrl = CF_SOLVER_URLS[currentSolverIndex];
  currentSolverIndex = (currentSolverIndex + 1) % CF_SOLVER_URLS.length;
  
  console.log(`[queue] Using solver URL: ${targetUrl}`);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SOLVE_TIMEOUT);

  try {
    const { fetch } = require('undici');
    const res = await fetch(targetUrl, {
      method:  'POST',
      headers: {
        'Content-Type':    'application/json',
        'Accept':          '*/*',
        'Accept-Encoding': 'deflate, gzip',
        'Authorization':   'Bearer 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',
        'User-Agent':      UA,
      },
      body:    JSON.stringify({ domain: TURNSTILE_URL, siteKey: TURNSTILE_KEY, mode: 'turnstile' }),
      signal:  controller.signal,
      dispatcher: proxyAgent,
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

  const jobId = queue[0];
  const job   = jobs.get(jobId);

  if (!job) {
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

  queue.shift(); 
  isProcessing = false;
  
  // Implement the waiting time before moving to the next job
  if (JOB_DELAY_MS > 0 && queue.length > 0) {
    console.log(`[queue] Resting for ${JOB_DELAY_MS}ms before next job...`);
    setTimeout(processNext, JOB_DELAY_MS);
  } else {
    setImmediate(processNext);
  }
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

    const existingJobId = ipIndex.get(ip);
    if (existingJobId) {
      const existing = jobs.get(existingJobId);
      if (existing) {
        if (existing.status === 'queued' || existing.status === 'processing') {
          return send(res, 200, { jobId: existingJobId });
        }
        if (existing.status === 'done') {
          return send(res, 200, { jobId: existingJobId });
        }
        if (ipIndex.get(ip) === existingJobId) ipIndex.delete(ip);
      } else {
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
      if (Date.now() - job.finishedAt > TOKEN_TTL_MS) {
        if (ipIndex.get(job.ip) === jobId) ipIndex.delete(job.ip);
        jobs.delete(jobId);
        return send(res, 200, { status: 'error', errorCode: 'E_TOKEN_EXPIRED' });
      }
      if (job.tokenUsed) {
        return send(res, 200, { status: 'error', errorCode: 'E_TOKEN_USED' });
      }
      job.tokenUsed = true;
      return send(res, 200, { status: 'done', token: job.token });
    }

    if (job.status === 'error') {
      return send(res, 200, { status: 'error', errorCode: job.errorCode });
    }

    return send(res, 200, { status: job.status, position, eta });
  }

  // POST /cancel/:jobId
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

process.on('SIGTERM', () => { server.close(() => process.exit(0)); });
