package com.transactionshield.common.enums;

/**
 * Risk classification derived from the aggregated fraud score (0–100).
 *
 *  LOW      0–29   e.g. only NightTransaction fired  (+20)
 *  MEDIUM  30–59   e.g. only HighAmount fired        (+50)
 *  HIGH    60–89   e.g. HighAmount + Night           (+70)
 *  CRITICAL 90–100 e.g. BlacklistedCountry alone     (+100, capped)
 */
public enum RiskLevel {
    LOW, MEDIUM, HIGH, CRITICAL;

    public static RiskLevel from(int cappedScore) {
        if (cappedScore >= 90) return CRITICAL;
        if (cappedScore >= 60) return HIGH;
        if (cappedScore >= 30) return MEDIUM;
        return LOW;
    }
}
