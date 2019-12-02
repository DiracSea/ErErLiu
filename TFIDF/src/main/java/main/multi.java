package main;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.linalg.Vector;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;

import static main.single.*;
import static org.apache.spark.sql.functions.*;


public class multi {
    public static class KeyWords implements Serializable {
        private String label;
        private Object[] key;
        private double[] value;

        public Object[] getKey() {
            return key;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public void setKey(Object[] key) {
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
    public static Dataset<Row> getValue(String path, String tw) {

        SparkSession spark = initSpark();

        single s = new single();
        String[] dir = s.findDir(path);

        Dataset<Row> tmp = s.initReddit(path, dir[0])
                .limit(1).withColumn("label", when(col("label").equalTo("Reddit"), "X"));
        Dataset<Row> reddit1;

        int i = 0;
        for (String d : dir) {
            i += 1;
            if (d.equals("movie") || i > 10) break;
            reddit1 = s.initReddit(path, d);

            tmp = tmp.union(reddit1);
        }
        Dataset<Row> twitter = s.initTwitter(tw);
        Dataset<Row> df = twitter.union(tmp);


        HashingTF hashingTF = new HashingTF()
                .setInputCol("filtered")
                .setOutputCol("rawFeatures");

        Dataset<Row> featurizedData = hashingTF.transform(df);
        featurizedData.show(5);

        // IDF is an Estimator which is fit on a dataset and produces an IDFModel
        IDF idf = new IDF()
                .setInputCol("rawFeatures")
                .setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData);

        // The IDFModel takes feature vectors (generally created from HashingTF or CountVectorizer) and scales each column
        Dataset<Row> rescaledData = idfModel.transform(featurizedData);
        rescaledData.show(5);

        // Get Top N data and filter deleted row

        // SparseVector s = new SparseVector();
        return rescaledData.select("label", "filtered", "features");
    }

    public static Dataset<Row> slice(String path, String in, String community){
        SparkSession spark = initSpark();
        Dataset<Row> df = getValue(path, in);

        JavaRDD<KeyWords> key1 = df
                .select("label", "filtered", "features")
                .javaRDD()
                .map(r -> {
                        String label = r.getString(0);
                        List<Object> fuckU = r.getList(1);
                        Object[] ff = fuckU.toArray();

                        Vector tmp = r.getAs(2);
                        double[] value = tmp.toSparse().values();

                        KeyWords keyWords = new KeyWords();
                        keyWords.setLabel(label);
                        keyWords.setKey(ff);
                        keyWords.setValue(value);
                        return keyWords;
                });
        Dataset<Row> key2 = spark.createDataFrame(
                key1,
                KeyWords.class
        ); // .withColumn("id", monotonically_increasing_id());

        UDF2 concatItems = new UDF2<Seq<Object>, Seq<Double>, Seq<String>>() {
            public Seq<String> call(final Seq<Object> col1, final Seq<Double> col2) throws Exception {
                ArrayList zipped = new ArrayList();

                for (int i = 0, listSize = col1.size(); i < listSize; i++) {
                    String subRow = col1.apply(i).toString() + ";" + col2.apply(i).toString();
                    zipped.add(subRow);
                }

                return scala.collection.JavaConversions.asScalaBuffer(zipped);
            }
        };
        spark.udf().register("concatItems",concatItems, DataTypes.StringType);
        Dataset<KeyWord> df2 = key2
                .select(col("label"),
                        callUDF("concatItems", col("key"), col("value")).alias("key_value"))
                .map(s -> {
                    String info = s.getList(1).toString();
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

    public static void main(String[] args) throws IOException {
        String input = args[0], output = args[1], tw = args[2], output1 = args[3];
        JavaRDD<String> df = slice(input, tw, "Twitter").toJSON().toJavaRDD();

        df.coalesce(1).saveAsTextFile(output+"T");
    }

}
