package org.example.service;

import org.example.model.Tweet;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Properties;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class HiveQueryingService {

    private static final String HIVE_JDBC_URL = "jdbc:hive2://localhost:10000/default";
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    public Connection getHiveConnection() throws SQLException, ClassNotFoundException {
        Class.forName(HIVE_DRIVER);
        
        // Retry connection with backoff for services that are starting up
        int maxRetries = 5;
        int retryDelayMs = 3000;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                System.out.printf("🔌 Attempting Hive connection (attempt %d/%d)...%n", attempt, maxRetries);
                Connection conn = DriverManager.getConnection(HIVE_JDBC_URL, "", "");
                System.out.println("✅ Successfully connected to Hive");
                return conn;
                
            } catch (SQLException e) {
                if (attempt == maxRetries) {
                    System.err.println("❌ Failed to connect to Hive after " + maxRetries + " attempts");
                    System.err.println("💡 Make sure Hive services are running:");
                    System.err.println("   docker compose up -d postgres hive-metastore hive-server");
                    System.err.println("   docker compose ps  # Check service status");
                    throw e;
                }
                
                System.out.printf("⚠️  Connection attempt %d failed: %s%n", attempt, e.getMessage());
                System.out.printf("⏳ Waiting %d seconds before retry...%n", retryDelayMs / 1000);
                
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Connection interrupted", ie);
                }
            }
        }
        
        throw new SQLException("Should not reach here");
    }

    public void createTweetsTable(String location) throws SQLException, ClassNotFoundException {
        // First, create the HDFS directory if it doesn't exist
        try {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("dfs.client.use.datanode.hostname", "false");
            conf.setBoolean("dfs.permissions.enabled", false);
            
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location);
            
            if (!fs.exists(path)) {
                System.out.println("🗂️ Creating HDFS directory: " + location);
                fs.mkdirs(path);
                System.out.println("✅ HDFS directory created successfully");
            }
            fs.close();
        } catch (java.io.IOException e) {
            System.err.println("⚠️ Warning: Could not create HDFS directory, table creation may fail: " + e.getMessage());
        }
        
        try (Connection conn = getHiveConnection();
             Statement stmt = conn.createStatement()) {
            
            // Drop existing table to ensure fresh creation with correct HDFS path
            System.out.println("🗑️ Dropping existing tweets table if it exists...");
            stmt.execute("DROP TABLE IF EXISTS tweets");
            System.out.println("✅ Existing table dropped");

            String createTableQuery = """
                CREATE TABLE IF NOT EXISTS tweets (
                    tweet_id STRING,
                    user_id STRING,
                    username STRING,
                    content STRING,
                    tweet_timestamp BIGINT,
                    like_count INT,
                    retweet_count INT,
                    reply_count INT,
                    is_celebrity BOOLEAN,
                    celebrity_category STRING,
                    hashtags ARRAY<STRING>,
                    mentions ARRAY<STRING>
                )
                STORED AS PARQUET
                LOCATION 'hdfs://namenode:9000%s'
                """.formatted(location);

            stmt.execute(createTableQuery);
            System.out.println("✅ Tweets table created successfully");
        }
    }

    public void createUsersTable(String location) throws SQLException, ClassNotFoundException {
        try (Connection conn = getHiveConnection();
             Statement stmt = conn.createStatement()) {

            String createTableQuery = """
                CREATE TABLE IF NOT EXISTS users (
                    user_id STRING,
                    username STRING,
                    display_name STRING,
                    follower_count BIGINT,
                    following_count BIGINT,
                    tweet_count INT,
                    is_verified BOOLEAN,
                    bio STRING,
                    category STRING
                )
                STORED AS PARQUET
                LOCATION '%s'
                """.formatted(location);

            stmt.execute(createTableQuery);
            System.out.println("✅ Users table created successfully");
        }
    }

    public List<Map<String, Object>> getMostLikedTweets(int limit) throws SQLException, ClassNotFoundException {
        String query = """
            SELECT tweet_id, content, like_count, username, celebrity_category
            FROM tweets
            WHERE like_count > 0
            ORDER BY like_count DESC
            LIMIT ?
            """;

        return executeQuery(query, limit);
    }

    public List<Map<String, Object>> getMostRetweetedTweets(int limit) throws SQLException, ClassNotFoundException {
        String query = """
            SELECT tweet_id, content, retweet_count, username, celebrity_category
            FROM tweets
            WHERE retweet_count > 0
            ORDER BY retweet_count DESC
            LIMIT ?
            """;

        return executeQuery(query, limit);
    }

    public List<Map<String, Object>> getMostPopularCelebrityTweets(String category, int limit) throws SQLException, ClassNotFoundException {
        String query = """
            SELECT tweet_id, content, like_count, retweet_count, username
            FROM tweets
            WHERE is_celebrity = true
            AND celebrity_category = ?
            ORDER BY (like_count + retweet_count) DESC
            LIMIT ?
            """;

        return executeQuery(query, category, limit);
    }

    public List<Map<String, Object>> getTrendingHashtags(int limit) throws SQLException, ClassNotFoundException {
        String query = """
            SELECT hashtag, COUNT(*) as frequency
            FROM tweets
            LATERAL VIEW explode(hashtags) hashtag_table AS hashtag
            GROUP BY hashtag
            ORDER BY frequency DESC
            LIMIT ?
            """;

        return executeQuery(query, limit);
    }

    public List<Map<String, Object>> getMostMentionedUsers(int limit) throws SQLException, ClassNotFoundException {
        String query = """
            SELECT mention, COUNT(*) as mention_count
            FROM tweets
            LATERAL VIEW explode(mentions) mention_table AS mention
            GROUP BY mention
            ORDER BY mention_count DESC
            LIMIT ?
            """;

        return executeQuery(query, limit);
    }

    public List<Map<String, Object>> getCelebrityEngagementStats() throws SQLException, ClassNotFoundException {
        String query = """
            SELECT 
                celebrity_category,
                COUNT(*) as tweet_count,
                AVG(like_count) as avg_likes,
                AVG(retweet_count) as avg_retweets,
                MAX(like_count) as max_likes
            FROM tweets
            WHERE is_celebrity = true
            GROUP BY celebrity_category
            ORDER BY avg_likes DESC
            """;

        return executeQuery(query);
    }

    private List<Map<String, Object>> executeQuery(String query, Object... params) throws SQLException, ClassNotFoundException {
        List<Map<String, Object>> results = new ArrayList<>();

        try (Connection conn = getHiveConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = rs.getObject(i);
                        row.put(columnName, value);
                    }
                    results.add(row);
                }
            }
        }

        return results;
    }



    public void batchInsertTweets(List<Tweet> tweets) throws SQLException, ClassNotFoundException {
        var partitionedTweets = tweets.stream().collect(Collectors.groupingBy(tweet -> {
            LocalDateTime dt = LocalDateTime.ofEpochSecond(tweet.getTimestamp(), 0, ZoneOffset.UTC);
            return String.format("%d/%d/%d/%d", dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour());
        }));
        
        System.out.println("📊 Inserting tweets into " + partitionedTweets.size() + " partitions using Calcite");
        
        try (var conn = getCalciteConnection()) {
            for (var entry : partitionedTweets.entrySet()) {
                var parts = entry.getKey().split("/");
                int year = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int day = Integer.parseInt(parts[2]);
                int hour = Integer.parseInt(parts[3]);
                
                insertTweetsToPartitionWithCalcite(conn, entry.getValue(), year, month, day, hour);
                System.out.println("✅ Inserted " + entry.getValue().size() + " tweets into partition " + entry.getKey());
            }
        }
        
        System.out.println("🎉 Batch insert completed for " + tweets.size() + " total tweets using Calcite");
    }
    
    private void insertTweetsToPartitionWithCalcite(Connection conn, List<Tweet> tweets, 
                                                   int year, int month, int day, int hour) throws SQLException {
        for (Tweet tweet : tweets) {
            // Build INSERT statement using Calcite SQL AST
            SqlNode insertNode = buildCalciteInsertStatement(tweet, year, month, day, hour);
            String generatedSql = insertNode.toString();
            
            try (PreparedStatement pstmt = conn.prepareStatement(generatedSql)) {
                setTweetParametersForCalcite(pstmt, tweet);
                pstmt.executeUpdate();
            }
        }
    }
    
    private SqlNode buildCalciteInsertStatement(Tweet tweet, int year, int month, int day, int hour) {
        // Create table identifier for tweets_partitioned
        SqlIdentifier tableName = new SqlIdentifier(
            Arrays.asList("hive", "tweets_partitioned"), 
            SqlParserPos.ZERO
        );
        
        // Create column list
        SqlNodeList columnList = new SqlNodeList(Arrays.asList(
            new SqlIdentifier("tweet_id", SqlParserPos.ZERO),
            new SqlIdentifier("user_id", SqlParserPos.ZERO),
            new SqlIdentifier("username", SqlParserPos.ZERO),
            new SqlIdentifier("content", SqlParserPos.ZERO),
            new SqlIdentifier("tweet_timestamp", SqlParserPos.ZERO),
            new SqlIdentifier("like_count", SqlParserPos.ZERO),
            new SqlIdentifier("retweet_count", SqlParserPos.ZERO),
            new SqlIdentifier("reply_count", SqlParserPos.ZERO),
            new SqlIdentifier("is_celebrity", SqlParserPos.ZERO),
            new SqlIdentifier("celebrity_category", SqlParserPos.ZERO),
            new SqlIdentifier("hashtags", SqlParserPos.ZERO),
            new SqlIdentifier("mentions", SqlParserPos.ZERO),
            new SqlIdentifier("year", SqlParserPos.ZERO),
            new SqlIdentifier("month", SqlParserPos.ZERO),
            new SqlIdentifier("day", SqlParserPos.ZERO),
            new SqlIdentifier("hour", SqlParserPos.ZERO)
        ), SqlParserPos.ZERO);
        
        // Create VALUES clause with placeholders
        SqlNodeList valuesList = new SqlNodeList(Arrays.asList(
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // tweet_id
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // user_id
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // username
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // content
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // timestamp
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // like_count
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // retweet_count
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // reply_count
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // is_celebrity
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // celebrity_category
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // hashtags
            SqlLiteral.createCharString("?", SqlParserPos.ZERO),  // mentions
            SqlLiteral.createExactNumeric(String.valueOf(year), SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric(String.valueOf(month), SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric(String.valueOf(day), SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric(String.valueOf(hour), SqlParserPos.ZERO)
        ), SqlParserPos.ZERO);
        
        // Create the VALUES expression with ROW
        SqlNode valuesExpression = SqlStdOperatorTable.VALUES.createCall(
            SqlParserPos.ZERO, 
            SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, valuesList)
        );
        
        // Create and return the INSERT statement
        return new SqlInsert(
            SqlParserPos.ZERO,
            SqlNodeList.EMPTY,  // keywords
            tableName,
            valuesExpression,
            columnList
        );
    }
    
    private void setTweetParametersForCalcite(PreparedStatement pstmt, Tweet tweet) throws SQLException {
        pstmt.setString(1, tweet.getTweetId());
        pstmt.setString(2, tweet.getUserId());
        pstmt.setString(3, tweet.getUsername());
        pstmt.setString(4, tweet.getContent());
        pstmt.setLong(5, tweet.getTimestamp());
        pstmt.setInt(6, tweet.getLikeCount());
        pstmt.setInt(7, tweet.getRetweetCount());
        pstmt.setInt(8, tweet.getReplyCount());
        pstmt.setBoolean(9, tweet.isCelebrity());
        pstmt.setString(10, tweet.getCelebrityCategory());
        
        // Handle arrays with Calcite
        Connection conn = pstmt.getConnection();
        Array hashtagsArray = conn.createArrayOf("VARCHAR", tweet.getHashtags().toArray());
        Array mentionsArray = conn.createArrayOf("VARCHAR", tweet.getMentions().toArray());
        pstmt.setArray(11, hashtagsArray);
        pstmt.setArray(12, mentionsArray);
    }
    
    
    
    public void createPartitionedTweetsTable(String location) throws SQLException, ClassNotFoundException {
        try (Connection conn = getHiveConnection();
             Statement stmt = conn.createStatement()) {
            
            String createTableQuery = """
                CREATE TABLE IF NOT EXISTS tweets_partitioned (
                    tweet_id STRING,
                    user_id STRING,
                    username STRING,
                    content STRING,
                    tweet_timestamp BIGINT,
                    like_count INT,
                    retweet_count INT,
                    reply_count INT,
                    is_celebrity BOOLEAN,
                    celebrity_category STRING,
                    hashtags ARRAY<STRING>,
                    mentions ARRAY<STRING>
                )
                PARTITIONED BY (
                    year INT,
                    month INT,
                    day INT,
                    hour INT
                )
                STORED AS PARQUET
                LOCATION '%s'
                """.formatted(location);
            
            stmt.execute(createTableQuery);
            System.out.println("✅ Partitioned tweets table created successfully");
        }
    }
    
    // Apache Calcite integration methods
    public Connection getCalciteConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties props = new Properties();
        
        // Set properties to handle Hive JDBC compatibility issues
        props.put("lex", "JAVA");
        props.put("conformance", "LENIENT");
        
        // Try to find calcite-model.json using classpath first (more reliable for tests)
        java.net.URL resourceUrl = getClass().getClassLoader().getResource("calcite-model.json");
        String configPath;
        
        if (resourceUrl != null) {
            // Convert URL to file path - Calcite seems to need actual file paths
            try {
                java.io.File file = new java.io.File(resourceUrl.toURI());
                configPath = file.getAbsolutePath();
                System.out.println("🔧 Using Calcite config from classpath file: " + configPath);
            } catch (java.net.URISyntaxException e) {
                // Fallback to URL string
                configPath = resourceUrl.toString();
                System.out.println("🔧 Using Calcite config from classpath URL: " + configPath);
            }
        } else {
            // Fallback to file system path
            String currentDir = System.getProperty("user.dir");
            if (currentDir.endsWith("/app")) {
                configPath = "file:" + currentDir + "/src/main/resources/calcite-model.json";
            } else {
                configPath = "file:" + currentDir + "/app/src/main/resources/calcite-model.json";
            }
            System.out.println("🔧 Using Calcite config from filesystem: " + configPath);
        }
        
        props.put("model", configPath);
        
        // Try connection with retry for compatibility issues
        int maxRetries = 3;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                System.out.println("🔗 Attempting Calcite connection (attempt " + attempt + "/" + maxRetries + ")...");
                Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
                System.out.println("✅ Calcite connection successful!");
                return conn;
                
            } catch (SQLException e) {
                if (attempt == maxRetries) {
                    System.err.println("❌ Failed to connect to Calcite after " + maxRetries + " attempts");
                    System.err.println("💡 This might be a Hive JDBC compatibility issue");
                    System.err.println("💡 Consider using direct Hive JDBC instead of Calcite for complex operations");
                    throw e;
                }
                
                System.out.printf("⚠️  Calcite connection attempt %d failed: %s%n", attempt, e.getMessage());
                System.out.printf("⏳ Retrying in 1 second...%n");
                
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Connection interrupted", ie);
                }
            }
        }
        
        throw new SQLException("Should not reach here");
    }
    
    public List<Tweet> queryTweetsWithCalculite(String sql) throws SQLException, ClassNotFoundException {
        List<Tweet> tweets = new ArrayList<>();
        
        try (Connection conn = getCalciteConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                Tweet tweet = new Tweet();
                tweet.setTweetId(rs.getString("tweet_id"));
                tweet.setUserId(rs.getString("user_id"));
                tweet.setUsername(rs.getString("username"));
                tweet.setContent(rs.getString("content"));
                tweet.setTimestamp(rs.getLong("tweet_timestamp"));
                tweet.setLikeCount(rs.getInt("like_count"));
                tweet.setRetweetCount(rs.getInt("retweet_count"));
                tweet.setReplyCount(rs.getInt("reply_count"));
                tweet.setCelebrity(rs.getBoolean("is_celebrity"));
                tweet.setCelebrityCategory(rs.getString("celebrity_category"));
                
                // Handle arrays - Calcite represents them differently
                Array hashtagsArray = rs.getArray("hashtags");
                if (hashtagsArray != null) {
                    String[] hashtags = (String[]) hashtagsArray.getArray();
                    tweet.setHashtags(Arrays.asList(hashtags));
                }
                
                Array mentionsArray = rs.getArray("mentions");
                if (mentionsArray != null) {
                    String[] mentions = (String[]) mentionsArray.getArray();
                    tweet.setMentions(Arrays.asList(mentions));
                }
                
                tweets.add(tweet);
            }
        }
        
        return tweets;
    }
    
    public List<Tweet> getMostLikedTweetsWithCalcite(int limit) throws SQLException, ClassNotFoundException {
        String sql = String.format("""
            SELECT tweet_id, user_id, username, content, tweet_timestamp,
                   like_count, retweet_count, reply_count, is_celebrity,
                   celebrity_category, hashtags, mentions
            FROM hive.tweets_partitioned
            WHERE like_count > 0
            ORDER BY like_count DESC
            LIMIT %d
            """, limit);
        
        return queryTweetsWithCalculite(sql);
    }
    
    public List<Map<String, Object>> getAdvancedAnalyticsWithCalcite() throws SQLException, ClassNotFoundException {
        String sql = """
            SELECT 
                celebrity_category,
                COUNT(*) as tweet_count,
                AVG(like_count) as avg_likes,
                AVG(retweet_count) as avg_retweets,
                MAX(like_count) as max_likes,
                MIN(like_count) as min_likes,
                STDDEV_POP(like_count) as stddev_likes
            FROM hive.tweets_partitioned
            WHERE is_celebrity = true
            GROUP BY celebrity_category
            HAVING COUNT(*) > 5
            ORDER BY avg_likes DESC
            """;
        
        return executeCalciteQuery(sql);
    }
    
    public List<Map<String, Object>> getTimeBasedAnalytics(int year, int month) throws SQLException, ClassNotFoundException {
        String sql = String.format("""
            SELECT 
                day,
                hour,
                COUNT(*) as tweet_count,
                AVG(like_count) as avg_likes,
                SUM(CASE WHEN is_celebrity = true THEN 1 ELSE 0 END) as celebrity_tweets
            FROM hive.tweets_partitioned
            WHERE year = %d AND month = %d
            GROUP BY day, hour
            ORDER BY day, hour
            """, year, month);
        
        return executeCalciteQuery(sql);
    }
    
    private List<Map<String, Object>> executeCalciteQuery(String sql) throws SQLException, ClassNotFoundException {
        List<Map<String, Object>> results = new ArrayList<>();
        
        try (Connection conn = getCalciteConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(columnName, value);
                }
                results.add(row);
            }
        }
        
        return results;
    }
    
    // Hybrid approach: Use both JDBC and Calcite
    public void performCompleteAnalysis() throws SQLException, ClassNotFoundException {
        System.out.println("🔍 Starting comprehensive tweet analysis...");
        
        // Use traditional JDBC for simple queries
        var mostLiked = getMostLikedTweets(10);
        System.out.println("📊 Top 10 most liked tweets found: " + mostLiked.size());
        
        // Use Calcite for complex analytics
        var analytics = getAdvancedAnalyticsWithCalcite();
        System.out.println("📈 Celebrity analytics computed: " + analytics.size() + " categories");
        
        // Time-based analysis with Calcite
        var timeAnalytics = getTimeBasedAnalytics(2024, 8);
        System.out.println("⏰ Time-based analytics: " + timeAnalytics.size() + " data points");
        
        System.out.println("✅ Complete analysis finished!");
    }
}
