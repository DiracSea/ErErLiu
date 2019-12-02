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
        Dataset<Row> df = spark.read().json(m+"/"+dir[0]+"/part-00000");
        Dataset<Row> tmp;
        int flag = 0;
        for (String d: dir) {
            if (d.equals("movie")) continue;
            if (flag == 0) {
                flag += 1;
                continue;
            }
            tmp = spark.read().json(m+"/"+d+"/part-00000");
            df = df.union(tmp);
        }


    }
}
