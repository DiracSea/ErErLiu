package kmeans;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;


public class kmeans {
    private static SparkSession sc = null;
    public static SparkSession initSC() {
        if (sc == null) {
            sc = new SparkSession
                    .builder()
                    .appName("KMeans")
                    .getOrCreate();
        }
        return sc;
    }
    public double run_cost(String input, int num_cluster, int iter) {
        SparkSession spark = initSC();
        JavaSparkContext jsc = new JavaSparkContext(spark);
        // Load and parse data
        JavaRDD<String> data = jsc.textFile(input);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
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
        jsc.stop();

        return SE;
    }

    public void run_kmeans(String input, int num_cluster, int iter, String output) {
        SparkSession spark = initSC();
        JavaSparkContext jsc = new JavaSparkContext(spark);
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

        Dataset<Row> df = parsedData.toDF("value");

        JavaRDD<String> com = df.union(res.toDF("label")).toJavaRDD.coalesce(1);

        com.saveAsTextFile(output);
        // smallest is best
        jsc.stop();
    }
}
