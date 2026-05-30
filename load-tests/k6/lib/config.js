/**
 * Shared configuration for all K6 scenarios.
 *
 * Override via environment variables:
 *   k6 run -e BASE_URL=http://host:8082 scenarios/baseline.js
 */
export const BASE_URL = __ENV.BASE_URL || 'http://localhost:8082';
export const TRANSACTIONS_URL = `${BASE_URL}/api/v1/transactions`;

/**
 * SLO definitions — shared across all scenarios so thresholds are consistent.
 *
 * Baseline SLOs (steady-state, ≤ 50 concurrent users):
 *   p95 latency < 100 ms
 *   p99 latency < 200 ms
 *   error rate  < 0.1 %
 *
 * Stress SLOs (high load, ≤ 500 concurrent users):
 *   p95 latency < 250 ms
 *   p99 latency < 500 ms
 *   error rate  < 1.0 %
 *
 * Spike SLOs (burst, 2× normal):
 *   p99 latency < 500 ms
 *   error rate  < 2.0 %
 */
export const SLO = {
    baseline: {
        'http_req_duration{scenario:baseline}': ['p(95)<100', 'p(99)<200'],
        'http_req_failed{scenario:baseline}':   ['rate<0.001'],
        'checks{scenario:baseline}':            ['rate>0.99'],
    },
    stress: {
        'http_req_duration{scenario:stress}': ['p(95)<250', 'p(99)<500'],
        'http_req_failed{scenario:stress}':   ['rate<0.01'],
        'checks{scenario:stress}':            ['rate>0.95'],
    },
    spike: {
        'http_req_duration{scenario:spike}': ['p(99)<500'],
        'http_req_failed{scenario:spike}':   ['rate<0.02'],
    },
    soak: {
        'http_req_duration{scenario:soak}': ['p(95)<150', 'p(99)<300'],
        'http_req_failed{scenario:soak}':   ['rate<0.001'],
        'checks{scenario:soak}':            ['rate>0.99'],
    },
};
