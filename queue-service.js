/**
 * Queue Service - Render Deployment (Updated)
 * 
 * Manages a fair queue system with Turnstile token generation.
 * - One IP address = one queue entry at a time
 * - Turnstile tokens generated fairly via queue
 * - Tokens cached for 60 seconds per IP
 * - All communication secured with PRIVATE_KEY
 * 
 * Deploy to Render with environment variables:
 *   PRIVATE_KEY=mykey123
 *   TURNSTILE_API_URL=https://cf-rp12.onrender.com/cf-clearance-scraper
 *   UPSTREAM_BASE=https://studyspark.pro
 *   REFERRER_BASE=pw-olive-kappa.vercel.app
 *   NODE_ENV=production
 *   PORT=3000
 */

import express from 'express';
import crypto from 'crypto';

const app = express();
app.use(express.json());

// ────────────────────────────────────────────────────────────────────────────
// CONFIG & SECURITY
// ────────────────────────────────────────────────────────────────────────────

const PRIVATE_KEY = process.env.PRIVATE_KEY || 'mykey123';
const TURNSTILE_API_URL = process.env.TURNSTILE_API_URL || 'https://yoursminato-cloud.hf.space/cf-clearance-scraper';
const UPSTREAM_BASE = process.env.UPSTREAM_BASE || 'https://studyspark.pro';
const REFERRER_BASE = process.env.REFERRER_BASE || 'pw-olive-kappa.vercel.app';
const PORT = process.env.PORT || 3000;

const TOKEN_CACHE_TTL = 60 * 1000; // 60 seconds
const TOKEN_GENERATION_TIME = 30 * 1000; // 30 seconds per token (for ETA calculation)
const CLEANUP_INTERVAL = 60 * 1000; // Clean up expired tokens every 60s

// ────────────────────────────────────────────────────────────────────────────
// QUEUE STATE (In-memory; use Redis for production)
// ────────────────────────────────────────────────────────────────────────────

const queue = new Map(); // { ip: { position, registeredAt, isProcessing, hasToken } }
const tokenCache = new Map(); // { ip: { token, expiresAt } }
const processingIp = new Map(); // { ip: true } — tracks who is currently getting a token

// ────────────────────────────────────────────────────────────────────────────
// HELPERS
// ────────────────────────────────────────────────────────────────────────────

/**
 * Extract client IP from request, handling proxies.
 * Supports: X-Forwarded-For, CF-Connecting-IP, direct socket
 */
function getClientIp(req) {
  const forwarded = req.headers['x-forwarded-for'];
  if (forwarded) return typeof forwarded === 'string' ? forwarded.split(',')[0].trim() : forwarded[0].trim();
  
  const cfIp = req.headers['cf-connecting-ip'];
  if (cfIp) return typeof cfIp === 'string' ? cfIp : cfIp[0];
  
  return (
    req.socket.remoteAddress ||
    req.connection.remoteAddress ||
    req.ip ||
    'unknown'
  );
}

/**
 * Verify PRIVATE_KEY from Authorization header.
 * Format: Authorization: Bearer mykey123
 */
function verifyPrivateKey(req) {
  const auth = req.headers.authorization || '';
  const [scheme, token] = auth.split(' ');
  return scheme === 'Bearer' && token === PRIVATE_KEY;
}

/**
 * Standardized error response.
 */
function errorResponse(res, status, errorId, message) {
  return res.status(status).json({
    ok: false,
    error: 'invalid_link',
    errorId,
    message,
  });
}

/**
 * Resolve queue position (1-indexed) for a given IP.
 */
function getQueuePosition(ip) {
  const sortedEntries = Array.from(queue.entries())
    .sort((a, b) => a[1].registeredAt - b[1].registeredAt);
  const idx = sortedEntries.findIndex(([queueIp]) => queueIp === ip);
  return idx >= 0 ? idx + 1 : null;
}

/**
 * Solve Turnstile and return token.
 * Uses configurable upstream and referrer.
 */
async function solveTurnstile() {
  const ua = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36';
  try {
    const res = await fetch(TURNSTILE_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Accept-Encoding': 'deflate, gzip',
        'User-Agent': ua,
      },
      body: JSON.stringify({
        url: `https://${REFERRER_BASE}/player`,
        siteKey: '0x4AAAAAACqytllG1rHL_Acz',
        mode: 'turnstile-min',
      }),
    });

    if (!res.ok) {
      console.error(`[Turnstile] HTTP ${res.status}`);
      return null;
    }

    const data = await res.json();
    return data.token || null;
  } catch (err) {
    console.error('[Turnstile] Error:', err.message);
    return null;
  }
}

/**
 * Process queue: generate token for first IP in queue if not processing.
 */
async function processQueue() {
  // Find first IP that doesn't have a token yet and is not being processed
  const sortedEntries = Array.from(queue.entries())
    .sort((a, b) => a[1].registeredAt - b[1].registeredAt);

  for (const [ip, entry] of sortedEntries) {
    if (entry.isProcessing || entry.hasToken) continue;

    // Mark as processing
    queue.set(ip, { ...entry, isProcessing: true });
    processingIp.set(ip, true);

    try {
      const token = await solveTurnstile();
      if (!token) {
        console.error(`[Queue] Failed to generate token for ${ip}`);
        queue.delete(ip); // Remove from queue on failure
        processingIp.delete(ip);
        continue;
      }

      // Cache token for 60 seconds
      tokenCache.set(ip, {
        token,
        expiresAt: Date.now() + TOKEN_CACHE_TTL,
      });

      // Mark as processed and remove from queue
      queue.delete(ip);
      processingIp.delete(ip);

      console.log(`[Queue] Token generated for ${ip}, cache valid for 60s`);
    } catch (err) {
      console.error(`[Queue] Error processing ${ip}:`, err.message);
      queue.delete(ip);
      processingIp.delete(ip);
    }
  }
}

/**
 * Clean up expired tokens from cache.
 */
function cleanupExpiredTokens() {
  const now = Date.now();
  let removed = 0;
  for (const [ip, cached] of Array.from(tokenCache.entries())) {
    if (cached.expiresAt < now) {
      tokenCache.delete(ip);
      removed++;
    }
  }
  if (removed > 0) {
    console.log(`[Cleanup] Removed ${removed} expired tokens`);
  }
}

// Process queue every 1 second
setInterval(processQueue, 1000);

// Cleanup expired tokens every 60 seconds
setInterval(cleanupExpiredTokens, CLEANUP_INTERVAL);

// ────────────────────────────────────────────────────────────────────────────
// ENDPOINTS
// ────────────────────────────────────────────────────────────────────────────

/**
 * POST /queue/register
 * 
 * Register IP address in queue (BACKEND ONLY).
 * Called by the API after initial authentication.
 * 
 * Headers:
 *   Authorization: Bearer mykey123
 * 
 * Response:
 *   { ok: true, message: "Registered in queue" }
 *   { ok: false, error: "invalid_link", errorId: "queue_already_registered" }
 */
app.post('/queue/register', (req, res) => {
  if (!verifyPrivateKey(req)) {
    return errorResponse(res, 401, 'queue_unauthorized', 'Invalid private key');
  }

  const ip = getClientIp(req);
  if (!ip || ip === 'unknown') {
    return errorResponse(res, 400, 'queue_invalid_ip', 'Could not determine client IP');
  }

  // Check if already registered
  if (queue.has(ip) || tokenCache.has(ip)) {
    return errorResponse(res, 409, 'queue_already_registered', 'IP already in queue or has active token');
  }

  // Add to queue
  queue.set(ip, {
    position: getQueuePosition(ip),
    registeredAt: Date.now(),
    isProcessing: false,
    hasToken: false,
  });

  console.log(`[Queue] Registered ${ip}, position ${getQueuePosition(ip)}`);

  return res.status(200).json({
    ok: true,
    message: 'Registered in queue',
  });
});

/**
 * GET /queue/status
 * 
 * Get queue position and ETA (FRONTEND - unauthenticated).
 * Called by player every 5 seconds while in queue.
 * 
 * Response:
 *   { ok: true, position: 5, eta: 150 }  // position 5, ~150 seconds ETA
 *   { ok: true, position: 0 }  // token ready for retrieval
 *   { ok: false, error: "invalid_link", errorId: "queue_not_registered" }
 */
app.get('/queue/status', (req, res) => {
  const ip = getClientIp(req);
  if (!ip || ip === 'unknown') {
    return errorResponse(res, 400, 'queue_invalid_ip', 'Could not determine client IP');
  }

  // Check if token is ready
  if (tokenCache.has(ip)) {
    return res.status(200).json({
      ok: true,
      position: 0, // Ready for retrieval
      eta: 0,
    });
  }

  // Check if in queue
  if (queue.has(ip)) {
    const position = getQueuePosition(ip);
    const eta = position * (TOKEN_GENERATION_TIME / 1000); // ~30 sec per token
    return res.status(200).json({
      ok: true,
      position,
      eta,
    });
  }

  // Not in queue and no token
  return errorResponse(res, 404, 'queue_not_registered', 'Not in queue; register first');
});

/**
 * POST /queue/get-token
 * 
 * Retrieve generated Turnstile token (BACKEND ONLY).
 * Called by the API after player confirms position is 0.
 * 
 * Headers:
 *   Authorization: Bearer mykey123
 * 
 * Response:
 *   { ok: true, token: "0x..." }
 *   { ok: false, error: "invalid_link", errorId: "queue_token_not_ready" }
 */
app.post('/queue/get-token', (req, res) => {
  if (!verifyPrivateKey(req)) {
    return errorResponse(res, 401, 'queue_unauthorized', 'Invalid private key');
  }

  const ip = getClientIp(req);
  if (!ip || ip === 'unknown') {
    return errorResponse(res, 400, 'queue_invalid_ip', 'Could not determine client IP');
  }

  // Check if token exists and is valid
  const cached = tokenCache.get(ip);
  if (!cached || cached.expiresAt < Date.now()) {
    tokenCache.delete(ip); // Clean up expired
    return errorResponse(res, 404, 'queue_token_not_ready', 'Token not ready or expired');
  }

  // Return token and remove from cache
  const token = cached.token;
  tokenCache.delete(ip);

  console.log(`[Queue] Token retrieved for ${ip}`);

  return res.status(200).json({
    ok: true,
    token,
  });
});

/**
 * POST /queue/reset
 * 
 * Remove IP from queue (for cleanup/errors).
 * Called by API if user initiates a new request while in queue.
 * 
 * Headers:
 *   Authorization: Bearer mykey123
 */
app.post('/queue/reset', (req, res) => {
  if (!verifyPrivateKey(req)) {
    return errorResponse(res, 401, 'queue_unauthorized', 'Invalid private key');
  }

  const ip = getClientIp(req);
  queue.delete(ip);
  tokenCache.delete(ip);
  processingIp.delete(ip);

  console.log(`[Queue] Reset for ${ip}`);

  return res.status(200).json({
    ok: true,
    message: 'Reset queue',
  });
});

/**
 * GET /health
 * 
 * Health check endpoint.
 */
app.get('/health', (req, res) => {
  res.status(200).json({
    ok: true,
    queue_size: queue.size,
    cached_tokens: tokenCache.size,
    processing: processingIp.size,
    upstream: UPSTREAM_BASE,
    referrer: REFERRER_BASE,
  });
});

/**
 * GET /
 * 
 * Root endpoint info.
 */
app.get('/', (req, res) => {
  res.status(200).json({
    ok: true,
    service: 'PW Sphere Queue Service',
    version: '1.0.0',
    endpoints: [
      'POST /queue/register',
      'GET /queue/status',
      'POST /queue/get-token',
      'POST /queue/reset',
      'GET /health',
    ],
  });
});

// ────────────────────────────────────────────────────────────────────────────
// ERROR HANDLING
// ────────────────────────────────────────────────────────────────────────────

app.use((err, req, res, next) => {
  console.error('[Error]', err.message);
  res.status(500).json({
    ok: false,
    error: 'invalid_link',
    errorId: 'internal_error',
  });
});

app.use((req, res) => {
  res.status(404).json({
    ok: false,
    error: 'invalid_link',
    errorId: 'not_found',
  });
});

// ────────────────────────────────────────────────────────────────────────────
// START SERVER
// ────────────────────────────────────────────────────────────────────────────

const server = app.listen(PORT, () => {
  console.log(`\n╔══════════════════════════════════════════════════════════╗`);
  console.log(`║          PW Sphere Queue Service Started                 ║`);
  console.log(`╠══════════════════════════════════════════════════════════╣`);
  console.log(`║ Port:              ${String(PORT).padEnd(45, ' ')}║`);
  console.log(`║ Private Key:       ${PRIVATE_KEY === 'mykey123' ? 'DEFAULT (CHANGE!)' : 'Custom Set'} ${' '.repeat(29)}║`);
  console.log(`║ Upstream:          ${UPSTREAM_BASE.padEnd(48, ' ')}║`);
  console.log(`║ Referrer:          ${REFERRER_BASE.padEnd(48, ' ')}║`);
  console.log(`║ Turnstile URL:     ${TURNSTILE_API_URL.substring(0, 46).padEnd(48, ' ')}║`);
  console.log(`╚══════════════════════════════════════════════════════════╝\n`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('\n[SIGTERM] Graceful shutdown initiated...');
  server.close(() => {
    console.log('[SIGTERM] Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('\n[SIGINT] Graceful shutdown initiated...');
  server.close(() => {
    console.log('[SIGINT] Server closed');
    process.exit(0);
  });
});

// Unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('[Unhandled Rejection]', reason);
});

process.on('uncaughtException', (err) => {
  console.error('[Uncaught Exception]', err);
  process.exit(1);
});

export default app;
