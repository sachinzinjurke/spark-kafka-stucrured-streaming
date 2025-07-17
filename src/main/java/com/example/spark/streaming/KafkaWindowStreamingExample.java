package com.example.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import static org.apache.spark.sql.functions.*;


import java.util.concurrent.TimeoutException;

public class KafkaWindowStreamingExample {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("spark streaming").config("spark.master", "local")
                .config("spark.sql.warehouse.dir", "file:///app/").getOrCreate();
        spark.conf().set("spark.sql.streaming.metricsEnabled", "true");
        spark.conf().set("spark.sql.shuffle.partitions", "4");
        spark.conf().set("spark.sql.streaming.fileSource.log.compactInterval", "4");
        spark.conf().set("spark.sql.streaming.fileSource.log.cleanupDelay", "4");
        spark.conf().set("spark.sql.streaming.minBatchesToRetain",2);


        Dataset<Row> rawData = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .option("startingOffsets", "latest") // From starting
                .load();

        Dataset<Row> messages = rawData.selectExpr("CAST(value AS STRING) as word");
        messages.printSchema();
        Dataset<Row> withTsDF = messages.withColumn("timestamp",current_timestamp());
        withTsDF.printSchema();
        Dataset<Row> windowAggregated = withTsDF
                .groupBy(window(col("timestamp"), "10 seconds", "5 seconds"))
                .agg(count("*").alias("message_count"));

        windowAggregated.printSchema();

        StreamingQuery query = windowAggregated
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Complete())
                .option("checkpointLocation", "C:\\tools\\checkpoint\\window")
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }
}
