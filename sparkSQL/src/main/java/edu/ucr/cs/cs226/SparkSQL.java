package edu.ucr.cs.cs226;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


public class SparkSQL {
    public void findAve(String input, String output) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                // .config("")
                .getOrCreate();

        /*spark.sqlContext().implicits();*/
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
        // operation
        // host 0; - 1; timestamp 2; ins 3; url 4; code 5; bytes 6
        df.createOrReplaceTempView("visit");
        Dataset<Row> avgBytes = df.sqlContext().sql("SELECT code, avg(bytes) as num from visit");
        avgBytes.show(5);
        // df.groupBy("code").avg("bytes");
        JavaRDD<String> res = avgBytes.toJavaRDD().map(s -> "Code " + s.getAs("code").toString() + ", average number of bytes = " + s.getAs("num").toString());
        res.saveAsTextFile(output);
    }

    public void findPair(String input, String output, long t0, long t1) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                // .config("")
                .getOrCreate();

        /*spark.sqlContext().implicits();*/
        // Schema
        String schema = "host tmp time ins url code bytes";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schema.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType,true);
            fields.add(field);
        }
        fields.set(2, DataTypes.createStructField("time", DataTypes.LongType, true));
        // fields.set(6, DataTypes.createStructField("bytes", DataTypes.DoubleType, true));
        StructType sch = DataTypes.createStructType(fields);

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "false")
                .option("delimiter", "\t")
                .schema(sch)
                .load(input);
        // operation
        // host 0; - 1; timestamp 2; ins 3; url 4; code 5; bytes 6
        Dataset<Row> res = df.select("time").filter("time > t0 and time < t1");
        long time_count = df.count();
        System.out.println("Number of time between t0 and t1 are "+time_count);
    }
}
