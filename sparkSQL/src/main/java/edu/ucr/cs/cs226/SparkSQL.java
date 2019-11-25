package edu.ucr.cs.cs226;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;


public class SparkSQL {
    public void findAve(String input, String output) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                // .config("")
                .getOrCreate();

        // Schema
        String schema = "host tmp timestamp ins url code bytes";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schema.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType,true);
            fields.add(field);
        }
        // fields.set(2, DataTypes.createStructField("timestamp", DataTypes.LongType, true));
        fields.set(6, DataTypes.createStructField("bytes", DataTypes.DoubleType, true));
        StructType sch = DataTypes.createStructType(fields);

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "false")
                .option("delimiter", "\t")
                .schema(sch)
                .load(input);
        // demon
        df.show(5);

        // operation
        // host 0; - 1; timestamp 2; ins 3; url 4; code 5; bytes 6
        df.groupBy("code").avg("bytes");
    }
}
