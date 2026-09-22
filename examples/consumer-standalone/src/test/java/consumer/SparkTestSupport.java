package consumer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Shared Spark session + tiny deterministic fixtures. One SparkSession per
 * JVM (the mutation engine requires a stable session across the loop).
 */
class SparkTestSupport {

    static final SparkSession spark = SparkSession.builder()
            .master("local[1]")
            .appName("spark-mutator-consumer")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate();

    static Path tempDir() {
        try {
            return Files.createTempDirectory("consumer-tests");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Dataset<Row> orders() {
        StructType schema = new StructType()
                .add("order_id", DataTypes.IntegerType)
                .add("customer_id", DataTypes.StringType)
                .add("amount", DataTypes.IntegerType);
        return spark.createDataFrame(java.util.Arrays.asList(
                RowFactory.create(1, "C1", 150),
                RowFactory.create(2, "C2", 50),
                RowFactory.create(3, "C1", 100),
                RowFactory.create(4, "C3", 250)), schema);
    }

    static Dataset<Row> customers() {
        StructType schema = new StructType()
                .add("customer_id", DataTypes.StringType)
                .add("customer_name", DataTypes.StringType);
        return spark.createDataFrame(java.util.Arrays.asList(
                RowFactory.create("C1", "Alice"),
                RowFactory.create("C2", "Bob")), schema);
    }

    static Dataset<Row> employees() {
        StructType schema = new StructType()
                .add("name", DataTypes.StringType)
                .add("dept", DataTypes.StringType)
                .add("salary", DataTypes.IntegerType);
        return spark.createDataFrame(java.util.Arrays.asList(
                RowFactory.create("Ann", "eng", 120),
                RowFactory.create("Bob", "eng", 100),
                RowFactory.create("Cid", "ops", 80)), schema);
    }

    static Dataset<Row> users() {
        StructType schema = new StructType()
                .add("user_id", DataTypes.StringType)
                .add("tier", DataTypes.StringType);
        return spark.createDataFrame(java.util.Arrays.asList(
                RowFactory.create("u1", "GOLD"),
                RowFactory.create("u2", "SILVER")), schema);
    }
}
