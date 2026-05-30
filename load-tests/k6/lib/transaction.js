/**
 * Transaction request builder and assertion helpers.
 */
import http from 'k6/http';
import { check } from 'k6';
import { TRANSACTIONS_URL } from './config.js';

const CURRENCIES = ['USD', 'EUR', 'GBP', 'TRY'];
const COUNTRIES  = ['US', 'DE', 'GB', 'TR', 'FR', 'NL', 'JP', 'CA'];

/**
 * Submits a single transaction and asserts HTTP 202.
 * Returns true if the check passed.
 */
export function submitTransaction(jwt, idempotencyKey = null) {
    const key = idempotencyKey || generateKey();

    const payload = JSON.stringify({
        idempotencyKey: key,
        userId: `user-${randomInt(1, 10000)}`,
        amount: (randomInt(1, 50000) / 100).toFixed(2),
        currency: randomPick(CURRENCIES),
        country: randomPick(COUNTRIES),
        deviceFingerprint: `fp-${key}`,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${jwt}`,
        },
        tags: { name: 'submitTransaction' },
    };

    const res = http.post(TRANSACTIONS_URL, payload, params);

    const passed = check(res, {
        'status is 202': (r) => r.status === 202,
        'transactionId present': (r) => {
            try { return JSON.parse(r.body).transactionId !== undefined; }
            catch { return false; }
        },
        'response time < 500ms': (r) => r.timings.duration < 500,
    });

    return { passed, status: res.status, duration: res.timings.duration };
}

/**
 * Submits a known-high-risk transaction (large amount, blacklisted country).
 * Expects 202 — scoring happens asynchronously.
 */
export function submitHighRiskTransaction(jwt) {
    const key = generateKey();
    const payload = JSON.stringify({
        idempotencyKey: key,
        userId: `user-${randomInt(1, 100)}`,
        amount: '25000.00',
        currency: 'USD',
        country: 'RU',        // blacklisted → HIGH/CRITICAL risk
        deviceFingerprint: `fp-${key}`,
    });

    const res = http.post(TRANSACTIONS_URL, payload, {
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
        tags: { name: 'highRiskTransaction' },
    });

    check(res, { 'high-risk accepted (202)': (r) => r.status === 202 });
    return res;
}

// ── Helpers ─────────────────────────────────────────────────────────────

function generateKey() {
    return `k6-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomPick(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
}
