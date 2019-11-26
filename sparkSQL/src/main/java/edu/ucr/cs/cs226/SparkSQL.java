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
        String schema = "host tmp time ins url code bytes";
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
         Dataset<Row> avgBytes = df.sqlContext().sql("SELECT code, avg(bytes) as avg from visit GROUP BY code");
         avgBytes.show(5);
        // df.groupBy("code").avg("bytes").as("num");
        // df.show(5);
        JavaRDD<String> res = avgBytes.toJavaRDD()
                .map(s -> "Code " + s.getAs("code").toString() + ", average number of bytes = " + s.getAs("avg").toString())
                .coalesce(1);
        res.saveAsTextFile(output);
        spark.close();
    }

    public void findPair(String input, String output, long t0, long t1) { // t0 < t1
        System.out.println((t0+t1)/2); // check input
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
        df.createOrReplaceTempView("visit");
        // filter("time > "+t0+" and time < "+t1); alternative
        Dataset<Row> a = df.sqlContext().sql("SELECT count(time) as t from visit where time > "+t0+" and time < "+t1);
        a.show(1);
        // long time_count = df.count(); alternative
        JavaRDD<String> res = a.toJavaRDD()
                .map(s -> "Logs between time0 and time1 are "+s.getAs("t").toString())
                .coalesce(1);
        res.saveAsTextFile(output);
        spark.close();
    }
    public static void main(String[] args) {
        // host 0; - 1; timestamp 2; ins 3; url 4; code 5; bytes 6
        /*
         * args[0] = "/data/input/nasa.tsv"
         * args[1] = output1
         * args[2] = t0
         * args[3] = t1
         */
        SparkSQL s = new SparkSQL();

        long start_time = System.nanoTime();
        s.findAve(args[0], args[1]);
        long end_time = System.nanoTime();
        double d = (end_time - start_time) / 1e6;

        start_time = System.nanoTime();
        s.findPair(args[0], args[2], Long.parseLong(args[3]), Long.parseLong(args[4]));
        end_time = System.nanoTime();
        double d1 = (end_time - start_time) / 1e6;

        start_time = System.nanoTime();

        end_time = System.nanoTime();
        double d2 = (end_time - start_time) / 1e6;

        System.out.println("time: " + d + "; time1: " + d1 + "; time2: " + d2);
    }
}
