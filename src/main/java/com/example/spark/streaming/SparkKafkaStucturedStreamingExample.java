package com.example.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.avro.functions.*;

public class SparkKafkaStucturedStreamingExample {


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
                .option("subscribe", "AVRO-TOPIC")
                .option("startingOffsets", "earliest") // From starting
                .load();
        String jsonFormatSchema=null;
        try {
             jsonFormatSchema = new String(
                    Files.readAllBytes(Paths.get("./src/main/resources/inventory.avsc")));
        } catch (IOException e) {
            e.printStackTrace();
        }


        Dataset<Row> inventoryDF = rawData.select(from_avro(col("value"),
                jsonFormatSchema).as("inventory"))
                .select("inventory.*");
        inventoryDF.printSchema();

        StreamingQuery query = inventoryDF
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .option("checkpointLocation", "D:\\spark\\streaming\\kafka\\checkpoint\\")
                .start();

        query.awaitTermination();

    }
}
