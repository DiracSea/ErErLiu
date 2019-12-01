package main;

import java.io.*;
import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;

public class multi {
    public static class KeyWords implements Serializable {
        private String key;
        private double[] value;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public double[] getValue() {
            return value;
        }

        public void setValue(double[] value) {
            this.value = value;
        }
    }
    public static void slice(String path, String src, String in){
        SparkSession spark = single.initSpark();
        Dataset<Row> df = single.getValue(path, in);
        Dataset<KeyWords> key1 = df
                .select("filtered", "features").filter("label = 'Twitter'")
                .map(r -> {
                    String label = r.getString(0);
                    Vector tmp = r.getAs(1);
                    double[] value = tmp.toSparse().values();

                    KeyWords keyWords = new KeyWords();
                    keyWords.setKey(label);
                    keyWords.setValue(value);
                    return keyWords;
                }, Encoders.bean(KeyWords.class));
        Dataset<Row> key2 = spark.createDataFrame(
                key1.toJavaRDD(),
                KeyWords.class
        ).withColumn("id", functions.monotonically_increasing_id());
        Dataset<Row> keyword = key2.javaRDD().flatMap((x, y) -> ).drop("id");
    }

    public static void main(String[] args) {
        String input = args[0], output = args[1], tw = args[2], output1 = args[3];

    }

}
