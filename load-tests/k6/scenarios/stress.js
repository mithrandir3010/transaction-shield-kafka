/**
 * Stress Test — Find the Breaking Point
 *
 * Gradually increases load until the SLOs breach or the service collapses.
 * Used to determine maximum sustainable throughput.
 *
 * Shape:
 *   1m   →  50 VUs  (warm-up)
 *   2m   → 100 VUs
 *   2m   → 200 VUs
 *   2m   → 350 VUs
 *   2m   → 500 VUs  (target peak)
 *   1m   →   0 VUs  (cool-down)
 *
 * SLOs at peak load (stress is less strict than baseline):
 *   p95 latency  < 250 ms
 *   p99 latency  < 500 ms
 *   error rate   < 1.0 %
 *
 * Expected failure signatures:
 *   - HikariCP connection timeout → 500/503 spike at ~200 VUs
 *   - Kafka producer buffer full  → 503 spike at ~350 VUs
 *   - Redis latency degrades      → p99 exceeds 500 ms
 *
 * Run:
 *   k6 run load-tests/k6/scenarios/stress.js
 */
import { sleep }            from 'k6';
import { generateJwt }      from '../lib/auth.js';
import { submitTransaction, submitHighRiskTransaction } from '../lib/transaction.js';
import { Rate, Trend }      from 'k6/metrics';

const highRiskRate = new Rate('high_risk_accepted');
const p99Trend     = new Trend('p99_custom_ms');

export const options = {
    scenarios: {
        stress: {
            executor: 'ramping-vus',
            stages: [
                { duration: '1m',  target: 50  },
                { duration: '2m',  target: 100 },
                { duration: '2m',  target: 200 },
                { duration: '2m',  target: 350 },
                { duration: '2m',  target: 500 },
                { duration: '1m',  target: 0   },
            ],
        },
    },
    thresholds: {
        'http_req_duration{scenario:stress}': ['p(95)<250', 'p(99)<500'],
        'http_req_failed{scenario:stress}':   ['rate<0.01'],
        'checks{scenario:stress}':            ['rate>0.95'],
    },
};

const JWT = generateJwt();

export default function () {
    // 90% normal, 10% high-risk transactions (mirrors realistic traffic mix)
    if (Math.random() < 0.9) {
        const { duration } = submitTransaction(JWT);
        p99Trend.add(duration);
    } else {
        const res = submitHighRiskTransaction(JWT);
        highRiskRate.add(res.status === 202);
    }
    sleep(0.2);
}
