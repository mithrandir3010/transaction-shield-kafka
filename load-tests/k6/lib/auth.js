/**
 * JWT token generation for load tests.
 *
 * Uses HS256 symmetric signing with the dev secret key.
 * In production CI, pass JWT_SECRET_KEY environment variable:
 *   k6 run -e JWT_SECRET_KEY=<secret> baseline.js
 */
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';

const DEFAULT_SECRET = 'dev-jwt-secret-changeme-minimum-32-bytes!!';

/**
 * Generates an HS256 JWT valid for 24 hours.
 * Called once at script init, reused across all VU iterations.
 */
export function generateJwt(secretKey = __ENV.JWT_SECRET_KEY || DEFAULT_SECRET) {
    const header  = encoding.b64encode(JSON.stringify({ alg: 'HS256', typ: 'JWT' }), 'rawurl');
    const now     = Math.floor(Date.now() / 1000);
    const payload = encoding.b64encode(JSON.stringify({
        sub: 'k6-load-test',
        iss: 'transaction-shield-loadtest',
        exp: now + 86400,
        iat: now,
    }), 'rawurl');

    const signingInput = `${header}.${payload}`;
    const signature    = toBase64Url(crypto.hmac('sha256', secretKey, signingInput, 'base64'));
    return `${signingInput}.${signature}`;
}

function toBase64Url(b64) {
    return b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}
