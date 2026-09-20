package pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * WP-27 proof example for the {@code DECIMAL_TO_DOUBLE} mutator (Project
 * mutationIndex 2): a single Decimal(38,10) projection whose precision the
 * type-downgrade mutant truncates past ~15 significant digits. The weak suite
 * (cent-level tolerance) lets the mutant survive; the hardened suite (exact
 * value) kills it.
 */
public final class PrecisionPipeline {

    private PrecisionPipeline() {
    }

    public static Dataset<Row> buildReport(Dataset<Row> ledger) {
        return ledger.select(
                ledger.col("entry_id"),
                ledger.col("balance").as("reported_balance"));
    }
}
