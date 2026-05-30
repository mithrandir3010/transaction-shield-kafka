/**
 * Spike Test — Sudden Traffic Burst
 *
 * Simulates a sudden surge (e.g. flash sale, payment provider retry storm).
 * The system must survive the spike without cascading failures and recover
 * back to normal latency within the recovery window.
 *
 * Shape:
 *   10s  →  20 VUs  (normal baseline)
 *   30s  → 300 VUs  (spike — 15× normal)
 *   1m   → 300 VUs  (sustain)
 *   10s  →  20 VUs  (recovery)
 *   2m   →  20 VUs  (verify recovery — p99 must drop back below 200ms)
 *
 * SLOs:
 *   During spike   p99 < 500 ms, error rate < 2 %
 *   During recovery p99 < 200 ms within 2 minutes of spike end
 *
 * Run:
 *   k6 run load-tests/k6/scenarios/spike.js
 */
import { sleep }            from 'k6';
import { generateJwt }      from '../lib/auth.js';
import { submitTransaction } from '../lib/transaction.js';
import { Trend }            from 'k6/metrics';

const recoveryLatency = new Trend('recovery_latency_ms');

export const options = {
    scenarios: {
        spike: {
            executor: 'ramping-vus',
            stages: [
                { duration: '10s', target: 20  },
                { duration: '30s', target: 300 },
                { duration: '1m',  target: 300 },
                { duration: '10s', target: 20  },
                { duration: '2m',  target: 20  },
            ],
        },
    },
    thresholds: {
        'http_req_duration{scenario:spike}': ['p(99)<500'],
        'http_req_failed{scenario:spike}':   ['rate<0.02'],
        // Recovery window: after the spike, p99 must return below 200ms
        recovery_latency_ms: ['p(99)<200'],
    },
};

const JWT = generateJwt();

// Tracks whether we're in the recovery phase
let startTime;

export function setup() {
    startTime = Date.now();
}

export default function () {
    const elapsed = (Date.now() - (startTime || Date.now())) / 1000;
    const inRecovery = elapsed > 100; // after spike + sustain (~100s)

    const { duration } = submitTransaction(JWT);

    if (inRecovery) {
        recoveryLatency.add(duration);
    }

    sleep(0.1);
}
