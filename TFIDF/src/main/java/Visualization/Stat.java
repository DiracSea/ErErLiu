package Visualization;

import main.single;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Stat {
    public static void main(String[] args) {
        String m = args[0];
        single s = new single();
        SparkSession spark = single.initSpark();
        String[] dir = s.findDir(m);
        for (String d: dir) {
            Dataset<Row> df = spark.read().json(m+"/"+d+"/part-00000");
        }

    }
}
