package io.github.wpunit13.mutator.hash;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * SHA-256 + trunc64 hashing utility shared by NodeCoordinate and MutantID.
 *
 * <p>Truncation to 64 bits takes the first 8 bytes of the SHA-256 digest,
 * rendered as a fixed-width 16-character lowercase hex string.
 */
public final class DeterministicHasher {

    private DeterministicHasher() {
    }

    /**
     * Hashes the canonical input with SHA-256 and truncates to the first
     * 8 bytes, rendered as a 16-character lowercase hex string.
     *
     * @param canonicalInput the canonical string to hash; may be empty, never null
     * @return 16-character lowercase hex representation of the first 8 digest bytes
     * @throws IllegalArgumentException if canonicalInput is null
     */
    public static String hashToHex(String canonicalInput) {
        if (canonicalInput == null) {
            throw new IllegalArgumentException("canonicalInput must not be null");
        }
        MessageDigest digest = sha256();
        byte[] full = digest.digest(canonicalInput.getBytes(StandardCharsets.UTF_8));
        // Build hex from the raw bytes directly; converting to a signed long and
        // calling Long.toHexString would sign-extend for high-bit-set values.
        StringBuilder hex = new StringBuilder(16);
        for (int i = 0; i < 8; i++) {
            hex.append(String.format("%02x", full[i] & 0xff));
        }
        return hex.toString();
    }

    /**
     * Computes the MutantID over the pipe-delimited concatenation of
     * filePath, plan-node coordinate hex, operator type and mutation index.
     */
    public static String computeMutantId(String filePath, String coordinateHex, String operatorType, int mutationIndex) {
        Objects.requireNonNull(filePath, "filePath must not be null");
        Objects.requireNonNull(coordinateHex, "coordinateHex must not be null");
        Objects.requireNonNull(operatorType, "operatorType must not be null");
        String canonical = filePath + "|" + coordinateHex + "|" + operatorType + "|" + mutationIndex;
        return hashToHex(canonical);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed present on every JVM; unreachable in practice.
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
