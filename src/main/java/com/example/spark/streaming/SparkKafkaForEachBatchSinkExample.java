package com.example.spark.streaming;

import com.example.spark.streaming.custom.CustomBatchWriter;
import com.example.spark.streaming.query.CustomQueryListener;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

public class SparkKafkaForEachBatchSinkExample {


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

        /*
        *  Adding Spark Streaming Query Listener
        */

        spark.streams().addListener(new CustomQueryListener());

        Dataset<Row> rawData = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .option("startingOffsets", "latest") // From starting
                .load();

        Dataset<Row> valueStream = rawData.selectExpr("CAST(value AS STRING) as word");
        valueStream.printSchema();
        //Dataset<Row> wordCountDF = valueStream.groupBy(col("word")).count();

        StreamingQuery query = valueStream
                .writeStream()
                //.foreachBatch(new CustomBatchWriter())
                .foreachBatch(new CustomBatchWriter())
                .outputMode(OutputMode.Append())
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .option("checkpointLocation", "C:\\tools\\checkpoint\\forEachBatch")
                .start();

        query.awaitTermination();

    }
}
