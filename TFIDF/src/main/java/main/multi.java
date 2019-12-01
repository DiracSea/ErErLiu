package main;

import java.io.*;
import java.util.ArrayList;


import org.apache.spark.ml.linalg.Vector;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.*;


public class multi {
    public static class KeyWords implements Serializable {
        private String[] key;
        private double[] value;

        public String[] getKey() {
            return key;
        }

        public void setKey(String[] key) {
            this.key = key;
        }

        public double[] getValue() {
            return value;
        }

        public void setValue(double[] value) {
            this.value = value;
        }
    }
    public static class KeyWord implements Serializable {
        private String key;
        private double value;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }
    }
    public static Dataset<Row> slice(String path, String in){
        SparkSession spark = single.initSpark();
        Dataset<Row> df = single.getValue(path, in);
        Dataset<KeyWords> key1 = df
                .select("filtered", "features").filter("label = 'Twitter'")
                .map(r -> {
                    String[] label = r.getString(0).split(",");
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

        UDF2 concatItems = new UDF2<Seq<String>, Seq<Double>, Seq<String>>() {
            public Seq<String> call(final Seq<String> col1, final Seq<Double> col2) throws Exception {
                ArrayList zipped = new ArrayList();

                for (int i = 0, listSize = col1.size(); i < listSize; i++) {
                    String subRow = col1.apply(i) + ";" + col2.apply(i).toString();
                    zipped.add(subRow);
                }

                return scala.collection.JavaConversions.asScalaBuffer(zipped);
            }
        };
        spark.udf().register("concatItems",concatItems, DataTypes.StringType);
        Dataset<KeyWord> df2 = key2
                .select(col("id"),
                        callUDF("concatItems", col("key"), col("value")).alias("key_value"))
                .map(s -> {
                    String info = s.getAs(1);
                    String key = info.split(";")[0];
                    double value = Double.parseDouble(info.split(";")[1]);

                    KeyWord keyWord = new KeyWord();
                    keyWord.setKey(key);
                    keyWord.setValue(value);

                    return keyWord;
                }, Encoders.bean(KeyWord.class));

        Dataset<Row> keyword = spark.createDataFrame(
                df2.toJavaRDD(),
                KeyWord.class
        );

        Dataset<Row> rank = keyword
                .groupBy(col("key")).agg(avg("value").alias("v"));
        return rank.select("key","v").orderBy(col("v").desc());
    }

    public static void main(String[] args) {
        String input = args[0], output = args[1], tw = args[2], output1 = args[3];
        Dataset<Row> df = slice(input, tw);
        df.toJSON().javaRDD().saveAsTextFile(output);

    }

}
