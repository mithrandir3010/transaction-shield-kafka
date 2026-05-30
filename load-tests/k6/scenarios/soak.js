/**
 * Soak Test — Endurance / Memory Leak Detection
 *
 * Runs a moderate load for an extended period to surface:
 *   - Memory leaks (JVM heap grows, GC pauses increase)
 *   - Connection pool exhaustion (HikariCP / Lettuce leak)
 *   - Redis key TTL drift (idempotency keys not evicted)
 *   - Kafka consumer lag accumulation
 *
 * Shape:
 *   5m  → 30 VUs  (warm-up)
 *   30m → 30 VUs  (soak — default; set SOAK_DURATION for longer runs)
 *   2m  →  0 VUs
 *
 * SLOs (must hold throughout the full run):
 *   p95 latency  < 150 ms (slightly relaxed vs baseline to account for GC pauses)
 *   p99 latency  < 300 ms
 *   error rate   < 0.1 %
 *   check rate   > 99 %
 *
 * Run (default 30m):
 *   k6 run load-tests/k6/scenarios/soak.js
 *
 * Run (custom duration):
 *   k6 run -e SOAK_DURATION=60m load-tests/k6/scenarios/soak.js
 */
import { sleep }            from 'k6';
import { generateJwt }      from '../lib/auth.js';
import { submitTransaction } from '../lib/transaction.js';

const soakDuration = __ENV.SOAK_DURATION || '30m';

export const options = {
    scenarios: {
        soak: {
            executor: 'ramping-vus',
            stages: [
                { duration: '5m',          target: 30 },
                { duration: soakDuration,  target: 30 },
                { duration: '2m',          target: 0  },
            ],
        },
    },
    thresholds: {
        'http_req_duration{scenario:soak}': ['p(95)<150', 'p(99)<300'],
        'http_req_failed{scenario:soak}':   ['rate<0.001'],
        'checks{scenario:soak}':            ['rate>0.99'],
    },
};

const JWT = generateJwt();

export default function () {
    submitTransaction(JWT);
    sleep(1);  // 1 req/s per VU — 30 VU → ~30 TPS steady
}
