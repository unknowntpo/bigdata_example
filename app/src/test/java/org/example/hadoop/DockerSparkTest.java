package org.example.hadoop;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that connects to Docker Compose Spark cluster and executes queries on Hadoop
 *
 * Works both ways:
 * - From IDE: Click test button → Spark cluster auto-starts via extension
 * - From Gradle: ./gradlew testSpark → Spark starts via Gradle tasks
 */
@Tag("docker-test")
@ExtendWith({HadoopTestExtension.class, SparkTestExtension.class})
public class DockerSparkTest {

    private SparkSession spark;

    @BeforeEach
    void setUp() {
        // Stop any existing SparkSession first
        if (spark != null) {
            spark.stop();
            spark = null;
        }

        // Create new Spark session connected to Docker Compose cluster
        spark = SparkSession.builder()
            .appName("DockerSparkTest")
            .master("spark://localhost:7077")
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/")
            .config("fs.defaultFS", "hdfs://localhost:9000")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.catalogImplementation", "in-memory")
            // Resource allocation settings for small test cluster
            .config("spark.executor.memory", "512m")
            .config("spark.executor.cores", "1")
            .config("spark.driver.memory", "512m")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
            // Configure executors to use Docker network for HDFS
            .config("spark.executorEnv.HADOOP_CONF_DIR", "/opt/bitnami/hadoop/etc/hadoop")
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
            .config("spark.hadoop.dfs.nameservices", "namenode")
            .config("spark.hadoop.dfs.namenode.rpc-address", "namenode:9000")
            // Fix lambda serialization by ensuring test classes are available on executors
            .config("spark.submit.deployMode", "client")
            .config("spark.driver.extraClassPath", System.getProperty("java.class.path"))
            .getOrCreate();

        System.out.println("Connected to Spark cluster at: spark://localhost:7077");
        System.out.println("Using HDFS warehouse: hdfs://localhost:9000/ (root directory)");
    }

    @AfterEach
    void tearDown() {
        // Clean up SparkSession after each test
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    void testSparkClusterConnection() {
        // Verify Spark session is connected to cluster
        assertNotNull(spark);
        assertEquals("spark://localhost:7077", spark.conf().get("spark.master"));

        // Test basic Spark operation
        Dataset<Long> longDF = spark.range(1, 11);
        Dataset<Row> df = longDF.toDF();
        assertEquals(10, df.count());
        System.out.println("✅ Spark cluster connection verified");
    }

    @Test
    void testCreateTableAndQuery() {
        // Create sample data for testing
        List<String> data = Arrays.asList(
            "1,Alice,25,Engineer",
            "2,Bob,30,Manager",
            "3,Charlie,35,Analyst",
            "4,Diana,28,Designer",
            "5,Eve,32,Developer"
        );

        // Create DataFrame from sample data
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // Convert string data to Row objects
        JavaRDD<Row> rowRDD = jsc.parallelize(data).map(new PersonMapper());

        // Define schema
        StructType schema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("age", DataTypes.IntegerType, false),
            DataTypes.createStructField("job", DataTypes.StringType, false)
        });

        Dataset<Row> peopleDF = spark.createDataFrame(rowRDD, schema);

        // DataFrame already has proper column names from schema

        // Create temporary table
        peopleDF.createOrReplaceTempView("people");
        System.out.println("Created temporary table 'people' with sample data");

        // Test basic SELECT query using count() to avoid collectAsList() serialization
        Dataset<Row> result = spark.sql("SELECT * FROM people WHERE age > 30");
        long count = result.count();

        assertEquals(2, count);
        System.out.println("✅ Query executed: Found " + count + " people over 30");
        
        // Test another simple query to verify table works
        long totalPeople = spark.sql("SELECT COUNT(*) FROM people").collectAsList().get(0).getLong(0);
        assertEquals(5, totalPeople);
        System.out.println("✅ Total people in table: " + totalPeople);
    }

    @Test
    void testSaveToHDFS() {
        // Create test data
        Dataset<Row> testDF = spark.range(1, 101)
            .withColumn("doubled", functions.col("id").multiply(2))
            .withColumn("name", functions.concat(functions.lit("test_"), functions.col("id")));

        String hdfsPath = "hdfs://localhost:9000/test/spark_data";

        // Save to HDFS
        testDF.write()
            .mode("overwrite")
            .option("header", "true")
            .csv(hdfsPath);

        System.out.println("✅ Data saved to HDFS: " + hdfsPath);

        // Read back from HDFS to verify
        Dataset<Row> readDF = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(hdfsPath);

        assertEquals(100, readDF.count());
        System.out.println("✅ Data read back from HDFS: " + readDF.count() + " records");

        // Show sample data manually
        List<Row> sampleRows = readDF.limit(5).collectAsList();
        System.out.println("Sample data:");
        for (Row row : sampleRows) {
            System.out.println("ID: " + row.get(0) + ", Doubled: " + row.get(1) + ", Name: " + row.get(2));
        }
    }

    @Test
    void testAggregationQuery() {
        // Create sales data for aggregation testing
        List<String> salesData = Arrays.asList(
            "2024,Q1,Electronics,1000",
            "2024,Q1,Books,500",
            "2024,Q1,Electronics,800",
            "2024,Q2,Electronics,1200",
            "2024,Q2,Books,600",
            "2024,Q2,Clothing,400"
        );

        JavaSparkContext jsc2 = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // Convert string data to Row objects
        JavaRDD<Row> salesRowRDD = jsc2.parallelize(salesData).map(new SalesMapper());

        // Define schema
        StructType salesSchema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("year", DataTypes.IntegerType, false),
            DataTypes.createStructField("quarter", DataTypes.StringType, false),
            DataTypes.createStructField("category", DataTypes.StringType, false),
            DataTypes.createStructField("amount", DataTypes.IntegerType, false)
        });

        Dataset<Row> salesDF = spark.createDataFrame(salesRowRDD, salesSchema);

        // Create temporary table
        salesDF.createOrReplaceTempView("sales");

        // Test aggregation query
        Dataset<Row> quarterlyTotals = spark.sql(
            "SELECT quarter, SUM(amount) as total_sales " +
            "FROM sales " +
            "GROUP BY quarter " +
            "ORDER BY quarter"
        );

        List<Row> totals = quarterlyTotals.collectAsList();
        assertEquals(2, totals.size());

        // Verify Q1 total (1000 + 500 + 800 = 2300)
        assertEquals(2300L, totals.get(0).getLong(1));
        // Verify Q2 total (1200 + 600 + 400 = 2200)
        assertEquals(2200L, totals.get(1).getLong(1));

        System.out.println("✅ Aggregation query executed successfully");
        
        // Print quarterly totals manually
        System.out.println("Quarterly totals:");
        for (Row total : totals) {
            System.out.println("Quarter: " + total.getString(0) + ", Total Sales: " + total.getLong(1));
        }
    }

    @Test
    void testCreateHiveTable() {
        // Create sample Twitter-like data
        List<String> tweetData = Arrays.asList(
            "1,alice,Hello world!,2024-01-01,100",
            "2,bob,Spark is awesome,2024-01-02,250",
            "3,charlie,Big data analytics,2024-01-03,180",
            "4,diana,Machine learning rocks,2024-01-04,320"
        );

        JavaSparkContext jsc3 = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // Convert string data to Row objects
        JavaRDD<Row> tweetsRowRDD = jsc3.parallelize(tweetData).map(new TweetMapper());

        // Define schema
        StructType tweetsSchema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("tweet_id", DataTypes.IntegerType, false),
            DataTypes.createStructField("username", DataTypes.StringType, false),
            DataTypes.createStructField("content", DataTypes.StringType, false),
            DataTypes.createStructField("created_at", DataTypes.StringType, false),
            DataTypes.createStructField("likes", DataTypes.IntegerType, false)
        });

        Dataset<Row> tweetsDF = spark.createDataFrame(tweetsRowRDD, tweetsSchema);

        // Save as Hive table to HDFS warehouse
        String tablePath = "hdfs://localhost:9000/user/hive/warehouse/tweets";

        tweetsDF.write()
            .mode("overwrite")
            .option("path", tablePath)
            .saveAsTable("tweets");

        System.out.println("✅ Created Hive table 'tweets' in HDFS warehouse");

        // Query the created table
        Dataset<Row> result = spark.sql(
            "SELECT username, COUNT(*) as tweet_count, AVG(likes) as avg_likes " +
            "FROM tweets " +
            "GROUP BY username " +
            "ORDER BY avg_likes DESC"
        );

        assertEquals(4, result.count());
        System.out.println("✅ Successfully queried Hive table from HDFS");
        
        // Print Twitter analytics results manually
        List<Row> twitterResults = result.collectAsList();
        System.out.println("Twitter Analytics Results:");
        for (Row row : twitterResults) {
            System.out.println("User: " + row.getString(0) + 
                             ", Tweet Count: " + row.getLong(1) + 
                             ", Avg Likes: " + String.format("%.1f", row.getDouble(2)));
        }
    }

}

// Completely independent utility classes outside the test class to avoid any serialization issues
class PersonMapper implements org.apache.spark.api.java.function.Function<String, Row> {
    @Override
    public Row call(String line) {
        String[] parts = line.split(",");
        return RowFactory.create(
            Integer.parseInt(parts[0]),
            parts[1],
            Integer.parseInt(parts[2]),
            parts[3]
        );
    }
}

class SalesMapper implements org.apache.spark.api.java.function.Function<String, Row> {
    @Override
    public Row call(String line) {
        String[] parts = line.split(",");
        return RowFactory.create(
            Integer.parseInt(parts[0]),
            parts[1],
            parts[2],
            Integer.parseInt(parts[3])
        );
    }
}

class TweetMapper implements org.apache.spark.api.java.function.Function<String, Row> {
    @Override
    public Row call(String line) {
        String[] parts = line.split(",");
        return RowFactory.create(
            Integer.parseInt(parts[0]),
            parts[1],
            parts[2],
            parts[3],
            Integer.parseInt(parts[4])
        );
    }
}
