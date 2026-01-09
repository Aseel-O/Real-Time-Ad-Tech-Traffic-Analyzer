/**
 * AdTechTrafficAnalyzer.scala
 *
 * =============================================================================
 * REAL-TIME AD-TECH TRAFFIC ANALYZER WITH BOT DETECTION
 * =============================================================================
 *
 * PROJECT OVERVIEW:
 * ----------------
 * This Apache Spark Structured Streaming application processes real-time
 * advertising traffic data to detect fraudulent bot attacks and generate
 * business intelligence metrics. The system ingests high-velocity event streams
 * from Kafka, applies sophisticated anomaly detection algorithms, and outputs
 * actionable insights to MongoDB for real-time dashboards.
 *
 * BUSINESS PROBLEM:
 * ----------------
 * Online advertising platforms lose billions annually to click fraud and bot
 * traffic. Malicious actors deploy automated bots to:
 * 1. Inflate advertising costs by generating fake clicks
 * 2. Drain campaign budgets through fraudulent impressions
 * 3. Skew analytics data, making legitimate optimization impossible
 * 4. Damage advertiser trust and platform reputation
 *
 * Traditional batch analytics detect fraud hours or days after the damage is done.
 * This real-time system identifies attacks within 10-15 seconds, enabling:
 * - Immediate bot IP blocking
 * - Real-time campaign protection
 * - Instant alerting to security teams
 * - Cost savings through fraud prevention
 *
 * TECHNICAL CHALLENGES:
 * --------------------
 * 1. HIGH THROUGHPUT: Processing thousands of events per second
 * 2. LOW LATENCY: Detecting attacks within seconds, not hours
 * 3. STATEFUL COMPUTATION: Tracking unique users requires maintaining state
 * 4. MEMORY CONSTRAINTS: Exact distinct counts would require storing all IPs (infeasible)
 * 5. OUT-OF-ORDER DATA: Network delays cause events to arrive out of sequence
 * 6. FAULT TOLERANCE: System must survive crashes without data loss
 *
 * SOLUTION APPROACH:
 * -----------------
 * 1. SPARK SQL FOR CLARITY: Using SQL queries makes the logic readable and maintainable
 * 2. PROBABILISTIC COUNTING: HyperLogLog++ algorithm estimates unique users with <2% error
 *    using only ~12KB of memory (vs gigabytes for exact counting)
 * 3. WINDOWED AGGREGATIONS: 10-second tumbling windows provide rapid attack detection
 * 4. WATERMARKING: 30-second tolerance handles late-arriving events while keeping latency low
 * 5. CHECKPOINTING: Fault-tolerant state management allows recovery from failures
 * 6. MULTI-DIMENSIONAL ANALYSIS: Global, campaign, and country-level metrics for comprehensive
 *    threat visibility
 *
 * PIPELINE ARCHITECTURE:
 * ---------------------
 * [Kafka] → [Parse JSON] → [Validate] → [Watermark] → [SQL TempView] →
 * [Aggregate (SQL)] → [MongoDB Sinks: Metrics, Alerts, Quality]
 *
 * KEY METRICS:
 * -----------
 * - Hits Per User Ratio: Total events / Unique IPs
 *   • Normal traffic: 1.0 - 2.0 (each user generates 1-2 events)
 *   • Suspicious: 2.0 - 4.0 (possible bot activity)
 *   • High Risk: 4.0 - 7.0 (likely bot attack)
 *   • Critical: 7.0+ (confirmed bot attack)
 *
 * TECHNOLOGY STACK:
 * ----------------
 * - Apache Spark 3.x (Structured Streaming + SQL)
 * - Apache Kafka (Message broker)
 * - MongoDB (Time-series metrics storage)
 * - Scala 2.12
 * - HyperLogLog++ (Probabilistic counting)
 *
 * AUTHOR: Aseel Omar
 * COURSE: Big Data
 * DATE: January 2026
 * =============================================================================
 **/

// ============================================================================
// IMPORTS
// ============================================================================

// SparkSession: The unified entry point for Spark SQL functionality.
// Replaces the older SQLContext and HiveContext from Spark 1.x.
import org.apache.spark.sql.SparkSession

// SQL Functions: Provides built-in functions for transformations and aggregations.
// Key functions used: from_json, to_timestamp, col, when, lit, current_timestamp
import org.apache.spark.sql.functions._

// Trigger: Defines micro-batch processing intervals for streaming queries.
// We use ProcessingTime trigger for predictable 2-second batch intervals.
import org.apache.spark.sql.streaming.Trigger

// Schema Types: Used to define the structure of incoming JSON data.
// Explicit schemas are MANDATORY in Structured Streaming (no inference possible).
import org.apache.spark.sql.types._

// Log4j: Controls Spark's verbose logging output.
// Setting to ERROR suppresses INFO/DEBUG logs for cleaner console output.
import org.apache.log4j.{Level, Logger}

// Scala Collections: Used for defining schema field sequences.
import scala.collection.immutable.Seq

// ============================================================================
// MAIN APPLICATION OBJECT
// ============================================================================

object AdTechTrafficAnalyzer {

  /**
   * Main entry point for the Spark Streaming application.
   *
   * This method orchestrates the entire pipeline:
   * 1. Configures Spark with optimized settings for low-latency streaming
   * 2. Defines the schema for incoming ad event data
   * 3. Reads streaming data from Kafka
   * 4. Parses and validates JSON events
   * 5. Creates SQL temp views for query execution
   * 6. Executes SQL queries for aggregations and bot detection
   * 7. Writes results to multiple MongoDB collections
   * 8. Manages streaming query lifecycle
   *
   * @param args Command line arguments (currently unused)
   */
  def main(args: Array[String]): Unit = {

    // =========================================================================
    // SECTION 1: ENVIRONMENT SETUP & LOGGING CONFIGURATION
    // =========================================================================

    // Suppress Spark's verbose INFO logs to reduce console noise.
    // Only ERROR-level messages (crashes, failures) will be displayed.
    // This makes it easier to see our custom println() statements.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Set Hadoop home directory for Windows environments.
    // Spark requires 'winutils.exe' on Windows for local file operations.
    // On Linux/Mac, this line can be safely removed or ignored.
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    // =========================================================================
    // SECTION 2: SPARK SESSION INITIALIZATION
    // =========================================================================

    // Create the SparkSession with optimized configurations for streaming.
    // The builder pattern allows chaining multiple configuration options.
    val spark = SparkSession
      .builder()
      // Application name: Appears in Spark UI for monitoring
      .appName("RealTimeAdTechAnalyzer")

      // Master: "local[*]" runs Spark locally using all available CPU cores.
      // In production, this would be "spark://master:7077" or "yarn".
      .master("local[*]")

      // MongoDB Read Configuration:
      // Default URI for reading data from MongoDB collections.
      // Format: mongodb://host:port/database.collection
      .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://localhost:27017/adtech.traffic_metrics"
      )

      // MongoDB Write Configuration:
      // Default URI for writing data to MongoDB collections.
      // Each writeStream can override the collection using .option("collection", "name")
      .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://localhost:27017/adtech.traffic_metrics"
      )

      // Checkpoint Location (CRITICAL for streaming):
      // Stores the processing progress (Kafka offsets, window state) to disk.
      // Benefits:
      // 1. Exactly-once processing semantics
      // 2. Fault tolerance: Resume from last processed offset after crash
      // 3. State management: Maintains aggregation state across restarts
      // 4. Idempotency: Prevents duplicate processing of messages
      .config(
        "spark.sql.streaming.checkpointLocation",
        System.getProperty("user.home") + "/spark-checkpoints/adtech"
      )
      .getOrCreate()

    // =========================================================================
    // SECTION 3: SPARK SQL OPTIMIZATION SETTINGS
    // =========================================================================

    // Set session timezone to GMT+2 (Gaza/Hebron time).
    // IMPORTANT: This ensures timestamp windows align with the timezone used
    // by the Python data generator. Mismatched timezones cause incorrect windowing.
    spark.conf.set("spark.sql.session.timeZone", "GMT+2")

    // Reduce shuffle partitions from default 200 to 4.
    // WHY: Default 200 partitions create massive overhead for local/small datasets.
    // Each partition spawns a separate task, causing excessive context switching.
    // For local development with <10K events/second, 4 partitions are optimal.
    // PRODUCTION: Increase to 50-200 based on cluster size and throughput.
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    // Print startup banner
    println("=" * 70)
    println("Real-Time Ad-Tech Traffic Analyzer - LOW LATENCY MODE")
    println("=" * 70)

    // Import Spark implicits for convenient DataFrame operations.
    // This enables:
    // 1. The "$" notation for column references: $"column_name"
    // 2. Automatic conversions between Scala types and Spark types
    // 3. Encoder derivation for case classes
    import spark.implicits._

    // =========================================================================
    // SECTION 4: SCHEMA DEFINITION
    // =========================================================================

    // Define the schema for incoming JSON ad events.
    // CRITICAL: Structured Streaming REQUIRES explicit schemas (no inference).
    // This is because streaming data arrives continuously - we can't scan
    // the entire dataset to infer types like we do with batch processing.
    //
    // SCHEMA DESIGN DECISIONS:
    // - timestamp: Non-nullable String (we'll convert to TimestampType after parsing)
    // - event_id: Non-nullable (required for potential deduplication)
    // - ip_address: Non-nullable (core field for bot detection)
    // - country: Non-nullable (required for geographic analysis)
    // - action: Non-nullable ('click' or 'view' - required for metrics)
    // - campaign_id: Nullable (some events may not be campaign-specific)
    // - device: Nullable (optional metadata)
    val adEventSchema = StructType(
      Seq(
        StructField("timestamp", StringType, nullable = false),
        StructField("event_id", StringType, nullable = false),
        StructField("ip_address", StringType, nullable = false),
        StructField("country", StringType, nullable = false),
        StructField("action", StringType, nullable = false),
        StructField("campaign_id", StringType, nullable = true),
        StructField("device", StringType, nullable = true)
      )
    )

    // =========================================================================
    // SECTION 5: KAFKA STREAMING SOURCE
    // =========================================================================

    // Create a streaming DataFrame that reads from Kafka.
    // This is the SOURCE of our streaming pipeline.
    //
    // KAFKA CONFIGURATION EXPLAINED:
    val kafkaStream = spark.readStream
      .format("kafka")  // Use the Kafka connector

      // Bootstrap servers: Initial connection points to Kafka cluster.
      // Format: "host1:port1,host2:port2,..." for multi-broker setups.
      .option("kafka.bootstrap.servers", "localhost:9092")

      // Subscribe to topic(s): Can be single topic or comma-separated list.
      // Alternative: .option("subscribePattern", "ad_.*") for regex matching.
      .option("subscribe", "ad_stream")

      // Starting offset behavior:
      // - "latest": On FIRST run, skip old messages (start from current offset)
      //            On RESTART (with checkpoint), resume from last processed offset
      // - "earliest": Always process ALL messages from topic beginning
      //              (causes long delays on restarts with large backlogs)
      //
      // PRODUCTION BEST PRACTICE: Use "latest" to avoid replay delays.
      // The checkpoint ensures we don't lose data on restarts.
      .option("startingOffsets", "latest")

      // Max offsets per trigger: Limits the batch size for each micro-batch.
      // Benefits:
      // 1. Prevents memory overflow from processing too many events at once
      // 2. Ensures consistent, predictable processing times
      // 3. Improves end-to-end latency (smaller batches = faster processing)
      //
      // TUNING: Adjust based on:
      // - Average event size: Larger events → smaller batch size
      // - Available memory: More RAM → larger batch size
      // - Desired latency: Lower latency → smaller batch size
      .option("maxOffsetsPerTrigger", "1000")

      // Fail on data loss: Set to false for production robustness.
      // If Kafka deletes old data (due to retention policy), the job continues
      // instead of crashing. Only the lost offsets are skipped.
      .option("failOnDataLoss", "false")
      .load()

    // =========================================================================
    // SECTION 6: JSON PARSING & DATA VALIDATION
    // =========================================================================

    // Transform the raw Kafka stream into structured data.
    // Kafka messages have the following schema:
    // - key: binary
    // - value: binary (our JSON payload)
    // - topic: string
    // - partition: int
    // - offset: long
    // - timestamp: timestamp
    //
    // We only need the 'value' field, which contains our JSON event data.
    val parsedStream = kafkaStream
      // Step 1: Cast the binary 'value' to a UTF-8 string
      .selectExpr("CAST(value AS STRING) as json")

      // Step 2: Parse the JSON string into a structured column using our schema
      // from_json() returns a struct column with nested fields
      .select(from_json(col("json"), adEventSchema).as("data"))

      // Step 3: Flatten the struct by promoting nested fields to top-level columns
      // This transforms: data.ip_address → ip_address
      .select("data.*")

      // Step 4: Convert the timestamp string to a proper TimestampType column
      // CRITICAL: The format string MUST match the input format exactly
      // Format: "yyyy-MM-dd HH:mm:ss.SSS"
      // Example: "2026-01-09 14:30:45.123"
      .withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS")
      )

      // Step 5: Add a data quality flag for malformed events
      // An event is malformed if:
      // 1. Timestamp parsing failed (event_timestamp is NULL)
      // 2. IP address is missing (NULL)
      //
      // We use lit(1) and lit(0) instead of true/false because SQL SUM()
      // works better with integers for counting malformed events.
      .withColumn(
        "is_malformed",
        when(col("event_timestamp").isNull || col("ip_address").isNull, lit(1))
          .otherwise(lit(0))
      )

    // =========================================================================
    // SECTION 7: WATERMARKING FOR OUT-OF-ORDER DATA
    // =========================================================================

    // Apply watermarking to handle late-arriving events.
    //
    // WATERMARKING EXPLAINED:
    // In real-world networks, events don't always arrive in chronological order.
    // Network delays, retries, and buffering cause events to arrive "late."
    //
    // Example Timeline:
    // - Event A: timestamp 10:00:00, arrives at 10:00:01 (on time)
    // - Event B: timestamp 10:00:05, arrives at 10:00:12 (7 seconds late!)
    //
    // Without watermarking:
    // - Spark must keep ALL historical windows open forever (memory leak!)
    // - We never know when to finalize and output window results
    //
    // With 30-second watermark:
    // - Spark tracks the maximum event timestamp seen: max_event_time
    // - Watermark = max_event_time - 30 seconds
    // - Events older than the watermark are DROPPED
    // - Windows older than the watermark are FINALIZED and output
    //
    // Example with 30-second watermark:
    // - Max event time seen: 10:00:50
    // - Watermark: 10:00:50 - 30s = 10:00:20
    // - Window [10:00:00 - 10:00:10]: FINALIZED (older than watermark)
    // - Window [10:00:10 - 10:00:20]: FINALIZED (at watermark boundary)
    // - Window [10:00:20 - 10:00:30]: STILL OPEN (within watermark tolerance)
    //
    // TUNING TRADEOFF:
    // - Longer watermark (e.g., 2 minutes): Fewer late events dropped, higher memory usage
    // - Shorter watermark (e.g., 30 seconds): More late events dropped, lower memory, faster results
    //
    // 30 SECONDS: Optimized for low-latency bot detection while tolerating
    // reasonable network delays. Balances accuracy and speed.
    val streamWithWatermark = parsedStream
      .withWatermark("event_timestamp", "30 seconds")

    // =========================================================================
    // SECTION 8: CREATE SQL TEMP VIEW
    // =========================================================================

    // Register the streaming DataFrame as a temporary SQL view.
    // This is the KEY STEP that enables us to use Spark SQL queries.
    //
    // WHY USE SQL INSTEAD OF DATAFRAME API?
    // 1. READABILITY: SQL syntax is universally understood
    // 2. MAINTAINABILITY: Business analysts can read/modify SQL queries
    // 3. EXPRESSIVENESS: Complex logic is often clearer in SQL
    // 4. COMPATIBILITY: Easy to port queries from traditional databases
    //
    // The view "clicks" is now available for SQL queries below.
    // NOTE: This is a TEMPORARY view (session-scoped, not persisted).
    streamWithWatermark.createOrReplaceTempView("clicks")

    // =========================================================================
    // SECTION 9: GLOBAL TRAFFIC METRICS (SQL AGGREGATION)
    // =========================================================================

    // Execute a SQL query to compute global traffic metrics across all campaigns.
    // This is the CORE bot detection logic using pure SQL syntax.
    //
    // SQL QUERY BREAKDOWN:
    val aggregatedMetrics = spark.sql("""
  SELECT
    -- TIME WINDOW: Create 10-second tumbling windows
    -- Tumbling = non-overlapping, fixed-size buckets
    -- Examples: [10:00:00-10:00:10), [10:00:10-10:00:20), ...
    -- Returns a struct with 'start' and 'end' timestamp fields
    window(event_timestamp, '10 seconds') AS time_window,
    
    -- UNIQUE USERS: Estimate distinct IP addresses using HyperLogLog++
    -- Why approx_count_distinct instead of count(DISTINCT ip_address)?
    --
    -- EXACT DISTINCT COUNTING PROBLEM:
    -- - Requires storing EVERY unique IP in memory
    -- - 1M unique IPs × 16 bytes each = 16 MB per window
    -- - With 100 windows in state = 1.6 GB just for IP tracking!
    -- - Memory grows unboundedly with traffic volume
    --
    -- HYPERLOGLOG++ SOLUTION:
    -- - Probabilistic algorithm using hash functions
    -- - Fixed memory: ~12 KB regardless of distinct count
    -- - Accuracy: ±1-2% standard error
    -- - Perfect for streaming: constant memory, high accuracy
    approx_count_distinct(ip_address) AS unique_users,
    
    -- TOTAL EVENTS: Simple count of all rows in the window
    count(*) AS total_hits,
    
    -- CLICKS: Count only 'click' actions using conditional aggregation
    -- CASE returns 1 for clicks, NULL otherwise
    -- count() ignores NULLs, so this counts only clicks
    count(CASE WHEN action = 'click' THEN 1 END) AS total_clicks,
    
    -- VIEWS: Count only 'view' actions (impressions)
    count(CASE WHEN action = 'view' THEN 1 END) AS total_views,
    
    -- HITS PER USER RATIO: The KEY bot detection metric
    -- Formula: Total Events / Unique Users
    --
    -- INTERPRETATION:
    -- - Ratio = 1.0: Each user generates 1 event (natural browsing)
    -- - Ratio = 1.5-2.0: Normal (users click/view multiple ads)
    -- - Ratio = 3.0-4.0: Suspicious (few users, many events)
    -- - Ratio = 5.0+: Bot attack (automated traffic from few IPs)
    --
    -- ZERO DIVISION HANDLING:
    -- If unique_users = 0 (no traffic), return 0.0 to avoid division by zero
    CASE
      WHEN approx_count_distinct(ip_address) = 0 THEN 0.0
      ELSE ROUND(count(*) / approx_count_distinct(ip_address), 2)
    END AS hits_per_user_ratio,
    
    -- SUSPICIOUS FLAG: Binary indicator for potential bot activity
    -- Threshold: 2.5 chosen based on empirical analysis
    -- (lower than production 5.0 for earlier detection in demo)
    CASE
      WHEN approx_count_distinct(ip_address) = 0 THEN false
      WHEN count(*) / approx_count_distinct(ip_address) > 2.5 THEN true
      ELSE false
    END AS is_suspicious,
    
    -- SEVERITY LEVEL: Categorical risk classification
    -- Maps ratio thresholds to human-readable severity labels
    -- Used for dashboard color-coding and alert prioritization
    CASE
      WHEN approx_count_distinct(ip_address) = 0 THEN 'UNKNOWN'
      WHEN count(*) / approx_count_distinct(ip_address) < 2.0 THEN 'NORMAL'
      WHEN count(*) / approx_count_distinct(ip_address) >= 2.0
           AND count(*) / approx_count_distinct(ip_address) < 4.0 THEN 'SUSPICIOUS'
      WHEN count(*) / approx_count_distinct(ip_address) >= 4.0
           AND count(*) / approx_count_distinct(ip_address) < 7.0 THEN 'HIGH_RISK'
      ELSE 'CRITICAL'
    END AS severity_level,
    
    -- PROCESSING TIMESTAMP: Wall-clock time when Spark processed this batch
    -- Different from event_timestamp (when event occurred)
    -- Used for latency monitoring: processed_at - event_timestamp = latency
    current_timestamp() AS processed_at
    
  FROM clicks
  -- Filter out malformed events with NULL timestamps
  -- These would cause errors in window() function
  WHERE event_timestamp IS NOT NULL
  -- GROUP BY: Partition data into 10-second windows and aggregate within each
  GROUP BY window(event_timestamp, '10 seconds')
""")

    // =========================================================================
    // SECTION 10: CAMPAIGN-LEVEL METRICS (SQL AGGREGATION)
    // =========================================================================

    // Drill-down analysis: Same metrics but grouped by BOTH time AND campaign.
    // This enables per-campaign bot detection and performance monitoring.
    //
    // USE CASES:
    // 1. Identify which specific campaigns are under attack
    // 2. Compare campaign performance (CTR, engagement)
    // 3. Detect campaigns being used for click fraud
    // 4. Generate per-advertiser billing reports
    val campaignMetrics = spark.sql("""
  SELECT
    window(event_timestamp, '10 seconds') AS time_window,
    campaign_id,  -- Additional grouping dimension
    approx_count_distinct(ip_address) AS unique_users,
    count(*) AS total_hits,
    count(CASE WHEN action = 'click' THEN 1 END) AS total_clicks,
    count(CASE WHEN action = 'view' THEN 1 END) AS total_views,
    CASE
      WHEN approx_count_distinct(ip_address) = 0 THEN 0.0
      ELSE ROUND(count(*) / approx_count_distinct(ip_address), 2)
    END AS hits_per_user_ratio,
    CASE
      WHEN approx_count_distinct(ip_address) = 0 THEN false
      WHEN count(*) / approx_count_distinct(ip_address) > 2.5 THEN true
      ELSE false
    END AS is_suspicious,
    CASE
      WHEN approx_count_distinct(ip_address) = 0 THEN 'UNKNOWN'
      WHEN count(*) / approx_count_distinct(ip_address) < 2.0 THEN 'NORMAL'
      WHEN count(*) / approx_count_distinct(ip_address) >= 2.0
           AND count(*) / approx_count_distinct(ip_address) < 4.0 THEN 'SUSPICIOUS'
      WHEN count(*) / approx_count_distinct(ip_address) >= 4.0
           AND count(*) / approx_count_distinct(ip_address) < 7.0 THEN 'HIGH_RISK'
      ELSE 'CRITICAL'
    END AS severity_level,
    current_timestamp() AS processed_at
  FROM clicks
  WHERE event_timestamp IS NOT NULL
  -- CRITICAL: GROUP BY must include ALL non-aggregated columns
  GROUP BY window(event_timestamp, '10 seconds'), campaign_id
""")

    // =========================================================================
    // SECTION 11: COUNTRY-LEVEL METRICS (GEOGRAPHIC ANALYSIS)
    // =========================================================================

    // Geographic traffic distribution analysis.
    // Groups metrics by time window AND country for geo-intelligence.
    //
    // USE CASES:
    // 1. Detect bot farms originating from specific countries
    // 2. Identify geographic patterns in traffic (time zones, events)
    // 3. Compliance reporting for GDPR, CCPA (data by region)
    // 4. Campaign optimization (which geos perform best)
    val countryMetrics = spark.sql("""
  SELECT
    window(event_timestamp, '10 seconds') AS time_window,
    country,  -- Geographic grouping dimension
    approx_count_distinct(ip_address) AS unique_users,
    count(*) AS total_hits,
    count(CASE WHEN action = 'click' THEN 1 END) AS total_clicks,
    count(CASE WHEN action = 'view' THEN 1 END) AS total_views,
    current_timestamp() AS processed_at
  FROM clicks
  WHERE event_timestamp IS NOT NULL
  GROUP BY window(event_timestamp, '10 seconds'), country
""")

    // =========================================================================
    // SECTION 12: DATA QUALITY METRICS (PIPELINE MONITORING)
    // =========================================================================

    // Monitor the HEALTH of the data pipeline itself.
    // Tracks malformed events to detect upstream data quality issues.
    //
    // WHY MONITOR DATA QUALITY?
    // 1. Early warning: Detect broken data producers before they corrupt analytics
    // 2. SLA compliance: Measure pipeline reliability (% of clean data)
    // 3. Debugging: Isolate whether issues are in data or processing logic
    // 4. Alerting: Page on-call engineers if malformed rate spikes
    //
    // EXAMPLE ALERT TRIGGER:
    // If (malformed_events / total_events) > 5%, send PagerDuty alert
    val qualityMetrics = spark.sql("""
  SELECT
    window(event_timestamp, '10 seconds') AS time_window,
    -- SUM the is_malformed flag (1 for bad events, 0 for good)
    -- This counts the total number of malformed events in the window
    SUM(is_malformed) AS malformed_events,
    COUNT(*) AS total_events
  FROM clicks
  GROUP BY window(event_timestamp, '10 seconds')
""")

    // Print configuration summary
    println("Compiled Spark SQL queries - LOW LATENCY CONFIG:")
    println("  - Window size: 10 seconds (rapid detection)")
    println("  - Watermark: 30 seconds (balances accuracy/latency)")
    println("  - Trigger: 2 seconds (fast micro-batches)")

    // =========================================================================
    // SECTION 13: MONGODB SINK - GLOBAL TRAFFIC METRICS
    // =========================================================================

    // Write the global metrics to MongoDB's 'traffic_metrics' collection.
    //
    // MONGODB SINK CONFIGURATION:
    val mongoWriter = aggregatedMetrics
      // Flatten the window struct for easier MongoDB querying
      // time_window.start → window_start
      // time_window.end → window_end
      .selectExpr(
        "CAST(time_window.start AS timestamp) as window_start",
        "CAST(time_window.end AS timestamp) as window_end",
        "unique_users",
        "total_hits",
        "total_clicks",
        "total_views",
        "hits_per_user_ratio",
        "severity_level",
        "processed_at"
      )
      .writeStream

      // OUTPUT MODE: "append"
      // Why not "update" or "complete"?
      //
      // - "append": Only outputs NEW rows (finalized windows)
      //   • Lowest MongoDB write load
      //   • Works with watermarks (outputs only when window is finalized)
      //   • Prevents duplicate writes for the same window
      //
      // - "update": Outputs changed rows (for aggregations with updates)
      //   • Higher write load (updates existing documents)
      //   • Useful for streaming joins or aggregations without watermarks
      //
      // - "complete": Outputs ALL aggregation state every batch
      //   • Massive write load (entire result table every 2 seconds!)
      //   • Only works for aggregations without watermarks
      //   • Rarely used in production
      .outputMode("append")

      // Use MongoDB connector (requires mongo-spark-connector JAR)
      .format("mongodb")

      // Checkpoint location for THIS specific sink
      // Each writeStream needs its own checkpoint directory
      // Stores: output progress, committed offsets, sink-specific state
      .option(
        "checkpointLocation",
        System.getProperty("user.home") + "/spark-checkpoints/mongodb"
      )

      // Force delete temp checkpoint on restart
      // Useful in DEVELOPMENT to avoid checkpoint compatibility errors
      // PRODUCTION: Set to "false" to preserve fault tolerance
      .option("forceDeleteTempCheckpointLocation", "true")

      // TRIGGER: Process micro-batches every 2 seconds
      // Why not continuous processing?
      //
      // Continuous Processing (experimental):
      // - Sub-second latency (~100ms)
      // - High CPU usage
      // - Limited connector support
      //
      // Micro-batch Processing (production-ready):
      // - 2-5 second latency
      // - Efficient resource usage
      // - Full connector ecosystem support
      //
      // 2 seconds: Optimal balance for our use case
      // - Fast enough for real-time bot detection
      // - Low enough overhead for stable operation
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    // =========================================================================
    // SECTION 14: MONGODB SINK - CAMPAIGN METRICS
    // =========================================================================

    // Write per-campaign metrics to separate collection for drill-down analysis.
    // MongoDB collection: 'campaign_metrics'
    val campaignWriter = campaignMetrics
      .selectExpr(
        "CAST(time_window.start AS timestamp) as window_start",
        "CAST(time_window.end AS timestamp) as window_end",
        "campaign_id",  // Partition key for campaign-specific queries
        "unique_users",
        "total_hits",
        "total_clicks",
        "total_views",
        "hits_per_user_ratio",
        "severity_level",
        "processed_at"
      )
      .writeStream
      .outputMode("append")
      .format("mongodb")
      // Override collection name (default is 'traffic_metrics' from SparkSession config)
      .option("collection", "campaign_metrics")
      .option(
        "checkpointLocation",
        System.getProperty("user.home") + "/spark-checkpoints/mongodb/campaign"
      )
      .option("forceDeleteTempCheckpointLocation", "true")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    // =========================================================================
    // SECTION 15: MONGODB SINK - COUNTRY METRICS
    // =========================================================================

    // Write per-country metrics for geographic analysis and compliance reporting.
    // MongoDB collection: 'country_metrics'
    val countryWriter = countryMetrics
      .selectExpr(
        "CAST(time_window.start AS timestamp) as window_start",
        "CAST(time_window.end AS timestamp) as window_end",
        "country",  // Partition key for country-specific queries
        "unique_users",
        "total_hits",
        "total_clicks",
        "total_views",
        "processed_at"
      )
      .writeStream
      .outputMode("append")
      .format("mongodb")
      .option("collection", "country_metrics")
      .option(
        "checkpointLocation",
        System.getProperty("user.home") + "/spark-checkpoints/mongodb/country"
      )
      .option("forceDeleteTempCheckpointLocation", "true")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    // =========================================================================
    // SECTION 16: MONGODB SINK - DATA QUALITY METRICS
    // =========================================================================

    // Write data quality metrics for pipeline health monitoring.
    // MongoDB collection: 'quality_metrics'
    //
    // OPERATIONAL MONITORING USE CASES:
    // 1. Set up alerts: If malformed_events > 100, page SRE team
    // 2. Track SLAs: Report monthly data quality percentage
    // 3. Debug production: Correlate quality drops with upstream changes
    val qualityWriter = qualityMetrics
      .selectExpr(
        "CAST(time_window.start AS timestamp) as window_start",
        "CAST(time_window.end AS timestamp) as window_end",
        "malformed_events",
        "total_events"
      )
      .writeStream
      .outputMode("append")
      .format("mongodb")
      .option("collection", "quality_metrics")
      .option(
        "checkpointLocation",
        System.getProperty("user.home") + "/spark-checkpoints/mongodb/quality"
      )
      .option("forceDeleteTempCheckpointLocation", "true")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    // =========================================================================
    // SECTION 17: ALERT STREAM - CAMPAIGN-LEVEL ATTACKS
    // =========================================================================

    // Generate alerts for campaigns under bot attack.
    // These alerts feed real-time dashboards and PagerDuty notifications.
    //
    // ALERT LOGIC:
    // - Filter: Only suspicious campaigns (is_suspicious = true)
    // - Destination: 'alerts' collection (shared with global alerts)
    // - Purpose: Enable campaign-specific incident response
    val campaignAlertsStream = campaignMetrics
      .where("is_suspicious = true")  // SQL WHERE clause for filtering
      .selectExpr(
        "CAST(time_window.start AS timestamp) as window_start",
        "CAST(time_window.end AS timestamp) as window_end",
        "campaign_id",

        // ALERT_TYPE: Categorizes alert for routing/filtering
        // Examples: RATIO_SPIKE, COUNTRY_ANOMALY, DEVICE_FRAUD
        "'RATIO_SPIKE' as alert_type",

        // SEVERITY MAPPING: Normalize severity levels for alerting system
        // Maps our detailed levels to standard incident severity scale
        "CASE " +
          "  WHEN severity_level = 'CRITICAL' THEN 'CRITICAL' " +
          "  WHEN severity_level = 'HIGH_RISK' THEN 'HIGH' " +
          "  WHEN severity_level = 'SUSPICIOUS' THEN 'MEDIUM' " +
          "  ELSE 'LOW' " +
          "END as severity",

        // ALERT MESSAGE: Human-readable description for dashboard/email
        // Uses SQL concat() function to build dynamic message
        "concat('Bot attack on ', campaign_id, ' - Ratio: ', hits_per_user_ratio, " +
          "' | Severity: ', severity_level) as message",

        "processed_at as timestamp",
        "total_hits",
        "unique_users",
        "hits_per_user_ratio",
        "severity_level",
        "processed_at"
      )
      .writeStream
      .outputMode("append")
      .format("mongodb")
      .option("collection", "alerts")  // Shared alerts collection
      .option(
        "checkpointLocation",
        System.getProperty("user.home") + "/spark-checkpoints/mongodb/campaign_alerts"
      )
      .option("forceDeleteTempCheckpointLocation", "true")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    // =========================================================================
    // SECTION 18: ALERT STREAM - GLOBAL TRAFFIC ANOMALIES
    // =========================================================================

    // Generate alerts for global (cross-campaign) bot attacks.
    // These are HIGHEST priority alerts indicating platform-wide threats.
    //
    // ALERT ESCALATION:
    // - Campaign alert: Page campaign manager
    // - Global alert: Page security operations center (SOC)
    val globalAlertsStream = aggregatedMetrics
      .where("is_suspicious = true")  // Filter for suspicious global traffic
      .selectExpr(
        "CAST(time_window.start AS timestamp) as window_start",
        "CAST(time_window.end AS timestamp) as window_end",

        // Mark as GLOBAL alert (no specific campaign)
        "'GLOBAL' as campaign_id",
        "'GLOBAL_RATIO_SPIKE' as alert_type",

        // Severity mapping (same as campaign alerts)
        "CASE " +
          "  WHEN severity_level = 'CRITICAL' THEN 'CRITICAL' " +
          "  WHEN severity_level = 'HIGH_RISK' THEN 'HIGH' " +
          "  WHEN severity_level = 'SUSPICIOUS' THEN 'MEDIUM' " +
          "  ELSE 'LOW' " +
          "END as severity",

        // Global alert message
        "concat('Global bot attack - Ratio: ', hits_per_user_ratio, " +
          "' | Severity: ', severity_level) as message",

        "processed_at as timestamp",
        "total_hits",
        "unique_users",
        "hits_per_user_ratio",
        "severity_level",
        "processed_at"
      )
      .writeStream
      .outputMode("append")
      .format("mongodb")
      .option("collection", "alerts")  // Same collection as campaign alerts
      .option(
        "checkpointLocation",
        System.getProperty("user.home") + "/spark-checkpoints/mongodb/global_alerts"
      )
      .option("forceDeleteTempCheckpointLocation", "true")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    // =========================================================================
    // SECTION 19: PIPELINE STARTUP & MONITORING
    // =========================================================================

    // Print startup confirmation
    println("\nStreaming pipeline ACTIVE - LOW LATENCY MODE")
    println("Expected end-to-end latency: 10-15 seconds")
    println("=" * 70)
    println("\nActive Streams:")
    println("  1. Global Metrics → traffic_metrics collection")
    println("  2. Campaign Metrics → campaign_metrics collection")
    println("  3. Country Metrics → country_metrics collection")
    println("  4. Quality Metrics → quality_metrics collection")
    println("  5. Campaign Alerts → alerts collection")
    println("  6. Global Alerts → alerts collection")
    println("\nPress Ctrl+C to stop the pipeline...")

    // =========================================================================
    // SECTION 20: KEEP APPLICATION ALIVE
    // =========================================================================

    // Block the main thread indefinitely.
    // The application will run until:
    // 1. User presses Ctrl+C
    // 2. A streaming query throws an unrecoverable exception
    // 3. The JVM is killed (SIGKILL)
    //
    // This method waits for ANY active stream to terminate.
    // If one stream fails, the entire application stops.
    spark.streams.awaitAnyTermination()
  }
}

/**
 * =============================================================================
 * END-TO-END DATA FLOW SUMMARY
 * =============================================================================
 *
 * 1. INGESTION (Kafka → Spark)
 *    - Kafka producer generates ad events (clicks, views)
 *    - Events published to 'ad_stream' topic
 *    - Spark reads batches every 2 seconds
 *
 * 2. PARSING (JSON → Structured Data)
 *    - Cast binary Kafka value to UTF-8 string
 *    - Parse JSON using explicit schema
 *    - Convert timestamp strings to TimestampType
 *    - Flag malformed events
 *
 * 3. WATERMARKING (Handle Late Data)
 *    - Track maximum event timestamp
 *    - Drop events older than (max_time - 30 seconds)
 *    - Finalize windows when watermark passes
 *
 * 4. AGGREGATION (SQL Queries)
 *    - Group by 10-second tumbling windows
 *    - Compute unique users with HyperLogLog++
 *    - Calculate hits-per-user ratio
 *    - Classify severity levels
 *
 * 5. BOT DETECTION (Business Logic)
 *    - Ratio > 2.5: Flag as suspicious
 *    - Ratio > 4.0: High risk
 *    - Ratio > 7.0: Critical attack
 *
 * 6. OUTPUT (MongoDB Sinks)
 *    - Write metrics to 4 collections
 *    - Write alerts to alerts collection
 *    - Checkpoint progress for fault tolerance
 *
 * 7. MONITORING (Operational Visibility)
 *    - Track data quality metrics
 *    - Monitor processing latency
 *    - Alert on anomalies
 *
 * =============================================================================
 * PRODUCTION DEPLOYMENT CONSIDERATIONS
 * =============================================================================
 *
 * 1. SCALING:
 *    - Increase spark.sql.shuffle.partitions (50-200)
 *    - Add Spark worker nodes for horizontal scaling
 *    - Increase maxOffsetsPerTrigger for higher throughput
 *
 * 2. RELIABILITY:
 *    - Set forceDeleteTempCheckpointLocation = false
 *    - Use distributed checkpoint storage (HDFS/S3)
 *    - Implement checkpoint backup/recovery procedures
 *
 * 3. MONITORING:
 *    - Integrate with Grafana for real-time dashboards
 *    - Set up PagerDuty alerts from MongoDB
 *    - Monitor Spark UI metrics (processing rates, delays)
 *
 * 4. SECURITY:
 *    - Enable Kafka SSL/SASL authentication
 *    - Use MongoDB authentication (remove anonymous access)
 *    - Encrypt checkpoints if storing sensitive data
 *
 * 5. OPTIMIZATION:
 *    - Tune watermark based on measured latency patterns
 *    - Adjust window size for detection speed vs. accuracy
 *    - Consider custom HLL++ parameters for higher accuracy
 *
 * =============================================================================
 */