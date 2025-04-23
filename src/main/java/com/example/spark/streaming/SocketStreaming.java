package com.example.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class SocketStreaming {

    //ncat -l -p 9999 --> This will listen to socket messages
    //ncat localhost 9999 --> from here you can send messages on socket

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        System.setProperty("hadoop.home.dir","C://winutils" );
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("spark streaming with socket").config("spark.master", "local")
                .config("spark.sql.warehouse.dir", "file:///app/").getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "192.168.1.6")
                .option("port", 9999)
                .load();

// Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
        words.printSchema();

// Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
