package kmeans;

import org.apache.commons.io.FileUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.sql.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;


public class kmeans {
    private static SparkSession spark = null;
    private static JavaSparkContext sc = null;
    public static JavaSparkContext initContext(){
        if (sc == null)
            sc = new JavaSparkContext(initSC().sparkContext());
        return sc;
    }
    public static SparkSession initSC() {
        if (spark == null) {
            spark = SparkSession
                    .builder()
                    .appName("KMeans")
                    .getOrCreate();
        }
        return spark;
    }
    public double run_cost(String input, int num_cluster, int iter) {
        SparkSession spark = initSC();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        // Load and parse data
        JavaRDD<String> data = jsc.textFile(input);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length-1];
            for (int i = 0; i < sarray.length-1; i++) {
                values[i] = Double.parseDouble(sarray[i+1]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();

        // Cluster the data into three classes using KMeans
        int numClusters = num_cluster;
        int numIterations = iter;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double SE = clusters.computeCost(parsedData.rdd());
        // smallest is best
//        jsc.stop();

        return SE;
    }
    public static class Line1 implements Serializable {
        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        private String value;

    }
    public static class Line2 implements Serializable {
        public int getLabel() {
            return label;
        }

        public void setLabel(int label) {
            this.label = label;
        }

        private int label;
    }

    public void run_kmeans(String input, int num_cluster, int iter, String output) {
        SparkSession spark = initSC();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        // Load and parse data
        JavaRDD<String> data = jsc.textFile(input);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length-1];
            for (int i = 0; i < sarray.length-1; i++) {
                values[i] = Double.parseDouble(sarray[i+1]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();

        // Cluster the data into three classes using KMeans
        int numClusters = num_cluster;
        int numIterations = iter;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        JavaRDD<Integer> res = clusters.predict(parsedData);

        Dataset<Row> df1 = spark
                .createDataFrame(parsedData.map(s -> s.toString()), Line1.class)
                .withColumn("id", functions.monotonically_increasing_id());
        Dataset<Row> df2 = spark
                .createDataFrame(res, Line2.class)
                .withColumn("id", functions.monotonically_increasing_id());

        Dataset<Row> df = df1
                .join(df2,  df1.col("id").equalTo(df2.col("id")))
                .drop("id");

        JavaRDD<String> com = df.toJavaRDD().map(s -> s.toString()).coalesce(1);

        com.saveAsTextFile(output);
        // smallest is best
//        jsc.stop();
    }
}
