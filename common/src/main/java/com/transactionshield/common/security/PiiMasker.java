package com.transactionshield.common.security;

/**
 * Stateless utility for masking PII before it reaches log sinks.
 *
 * Primary defense: call these methods at every log call-site.
 * Secondary defense: logback MaskingJsonGeneratorDecorator regex safety-net.
 */
public final class PiiMasker {

    private PiiMasker() {}

    /**
     * "user-001" → "us***01", "ab" → "***"
     * Safe to call with null.
     */
    public static String maskUserId(String userId) {
        if (userId == null || userId.length() <= 4) return "***";
        return userId.substring(0, 2) + "***" + userId.substring(userId.length() - 2);
    }

    /**
     * Shows only the last 4 digits: "4111111111111111" → "****-****-****-1111"
     * Safe to call with null.
     */
    public static String maskCardNumber(String cardNumber) {
        if (cardNumber == null) return "****";
        String digits = cardNumber.replaceAll("[^0-9]", "");
        if (digits.length() < 4) return "****";
        return "****-****-****-" + digits.substring(digits.length() - 4);
    }
}
