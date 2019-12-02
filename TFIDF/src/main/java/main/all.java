package main;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class all {
    public static void main(String[] args) {
        String input = args[0], output = args[1], tw = args[2], output1 = args[3];

        single s = new single();
        // similarDataset(getValue(input, tw));

        Dataset<Row> init = s.getValue(input, tw);
        JavaRDD<String> res = init.toJSON().toJavaRDD();

        res.saveAsTextFile(output);
    }
}
