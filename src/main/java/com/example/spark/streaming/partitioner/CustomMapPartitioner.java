package com.example.spark.streaming.partitioner;

import com.example.spark.streaming.pojo.Person;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CustomMapPartitioner implements MapPartitionsFunction<Row, Person> {
    @Override
    public Iterator<Person> call(Iterator<Row> partition) throws Exception {
        List<Person> transformedRows = new ArrayList<>();
        while (partition.hasNext()){
            Row row = partition.next();
            String word = row.getString(0);
            Person person = new Person();
            person.setName(word);
            person.setSurname(word+"_surname");
            System.out.println("Processing row for word : " + word);
            transformedRows.add(person);
        }
        return transformedRows.iterator();
    }

}
