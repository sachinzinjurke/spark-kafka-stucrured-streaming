package com.example.spark.ui;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class SparkUi {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("File Reading")
                .master("local[*]")
                .getOrCreate();

        StructType countrySchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("COUNTRY_ID", DataTypes.IntegerType, false),
                DataTypes.createStructField("NAME", DataTypes.StringType, false),
                DataTypes.createStructField("NATIONALITY", DataTypes.StringType, false),
                DataTypes.createStructField("COUNTRY_CODE", DataTypes.StringType, false),
                DataTypes.createStructField("ISO_ALPHA2", DataTypes.StringType, false),
                DataTypes.createStructField("CAPITAL", DataTypes.StringType, false),
                DataTypes.createStructField("POPULATION", DataTypes.DoubleType, false),
                DataTypes.createStructField("AREA_KM2", DataTypes.IntegerType, false),
                DataTypes.createStructField("REGION_ID", DataTypes.IntegerType, true),
                DataTypes.createStructField("SUB_REGION_ID", DataTypes.IntegerType, true),
                DataTypes.createStructField("INTERMEDIATE_REGION_ID", DataTypes.IntegerType, true),
                DataTypes.createStructField("ORGANIZATION_REGION_ID", DataTypes.IntegerType, true)
        });

        Dataset<Row> countries = spark.read()
                .option("header", "true")
                //.option("sep","\t")
                .schema(countrySchema)
                .csv("C:\\interview-workspace\\DATASETS\\data\\countries.csv");
        Dataset<Row> regionId = countries.filter(col("REGION_ID").equalTo(30));
        Dataset<Row> regionId1 = countries.groupBy(col("REGION_ID")).count();
        regionId1.show();
        regionId.show();

        Thread.sleep(50000);
        spark.close();
    }
}
