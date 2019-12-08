package kmeans;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static kmeans.kmeans.initSC;

public class label {
    public void removeLabel(String input, String output) {
        SparkConf conf = initSC();
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // Load and parse data
        JavaRDD<String> data = jsc.textFile(input);
        JavaRDD<String> res = data.map(s -> {
            int l = s.length();
            if (s.substring(0).equals("T"))
                return s.substring(8, l);
            else
                return s.substring(7, l);
        }).coalesce(1);

        res.saveAsTextFile(output);
    }

    public void setValue(String input, String output) {
        SparkConf conf = initSC();
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // Load and parse data
        JavaRDD<String> data = jsc.textFile(input);
        JavaRDD<String> res = data.map(s -> {
            int l = s.length();
            if (s.substring(0).equals("T"))
                return s.substring(8, l)+",Twitter";
            else
                return s.substring(7, l)+",Reddit";
        }).coalesce(1);

        res.saveAsTextFile(output);
    }
    public static void main(String[] args) {
        String input = args[0], o = args[1];
        label l = new label();
        l.setValue(input, o);
    }
}
