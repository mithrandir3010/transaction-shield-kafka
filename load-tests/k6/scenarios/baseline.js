/**
 * Baseline Load Test — Steady State
 *
 * Goal: verify the system comfortably handles normal production traffic.
 *
 * Shape:
 *   30s ramp-up   → 50 VUs
 *   2m steady     → 50 VUs  (≈ 100–150 TPS depending on response times)
 *   30s ramp-down → 0 VUs
 *
 * SLOs (MUST pass for green build):
 *   p95 latency  < 100 ms
 *   p99 latency  < 200 ms
 *   error rate   < 0.1 %
 *   check rate   > 99 %
 *
 * Run:
 *   k6 run load-tests/k6/scenarios/baseline.js
 *   k6 run -e BASE_URL=http://staging:8082 load-tests/k6/scenarios/baseline.js
 */
import { sleep }            from 'k6';
import { generateJwt }      from '../lib/auth.js';
import { submitTransaction } from '../lib/transaction.js';

export const options = {
    scenarios: {
        baseline: {
            executor: 'ramping-vus',
            stages: [
                { duration: '30s', target: 50 },
                { duration: '2m',  target: 50 },
                { duration: '30s', target: 0  },
            ],
        },
    },
    thresholds: {
        'http_req_duration{scenario:baseline}': ['p(95)<100', 'p(99)<200'],
        'http_req_failed{scenario:baseline}':   ['rate<0.001'],
        'checks{scenario:baseline}':            ['rate>0.99'],
    },
};

// Generate one JWT per VU, reuse for all iterations (token lives 24h)
const JWT = generateJwt();

export default function () {
    submitTransaction(JWT);
    sleep(0.3);   // ~3 req/s per VU → 50 VU → ~150 TPS
}
