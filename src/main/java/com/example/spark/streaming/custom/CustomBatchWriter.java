package com.example.spark.streaming.custom;

import com.example.spark.streaming.partitioner.CustomMapPartitioner;
import com.example.spark.streaming.pojo.Person;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

public class CustomBatchWriter implements VoidFunction2<Dataset<Row>,Long> {
    @Override
    public void call(Dataset<Row> inputStreamDataset, Long batchId) throws Exception {

        System.out.println("Processing in forEachBatch with batch id :: " + batchId);
        System.out.println("Total Count processed in batch:: " + inputStreamDataset.count());
        inputStreamDataset.show();
        Dataset<Person> transformedDS = inputStreamDataset.mapPartitions(new CustomMapPartitioner(), Encoders.bean(Person.class));
        transformedDS.show();
    }
}
