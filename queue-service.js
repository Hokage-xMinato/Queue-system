/**
 * Queue Service - Render Deployment
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
 *   NODE_ENV=production
 */

import express from 'express';
import crypto from 'crypto';

const app = express();
app.use(express.json());

// ────────────────────────────────────────────────────────────────────────────
// CONFIG & SECURITY
// ────────────────────────────────────────────────────────────────────────────

const PRIVATE_KEY = process.env.PRIVATE_KEY || 'mykey123';
const TURNSTILE_API_URL = process.env.TURNSTILE_API_URL || 'https://cf-rp12.onrender.com/cf-clearance-scraper';
const PORT = process.env.PORT || 3000;
const TOKEN_CACHE_TTL = 60 * 1000; // 60 seconds
const TOKEN_GENERATION_TIME = 30 * 1000; // 30 seconds per token (for ETA calculation)

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
 */
function getClientIp(req) {
  return (
    req.headers['x-forwarded-for']?.split(',')[0].trim() ||
    req.headers['cf-connecting-ip'] ||
    req.socket.remoteAddress ||
    req.connection.remoteAddress ||
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
        url: 'https://studyspark.site/player',
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

// Process queue every 1 second
setInterval(processQueue, 1000);

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
 * Health check
 */
app.get('/health', (req, res) => {
  res.status(200).json({
    ok: true,
    queue_size: queue.size,
    cached_tokens: tokenCache.size,
  });
});

// ────────────────────────────────────────────────────────────────────────────
// START SERVER
// ────────────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`[Queue Service] Listening on port ${PORT}`);
  console.log(`[Queue Service] PRIVATE_KEY configured: ${PRIVATE_KEY === 'mykey123' ? 'DEFAULT (CHANGE ME!)' : 'custom'}`);
  console.log(`[Queue Service] Turnstile URL: ${TURNSTILE_API_URL}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[Queue Service] SIGTERM received, shutting down gracefully...');
  process.exit(0);
});
