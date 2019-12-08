package kmeans;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.KMeans;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;


public class kmeans {
    private static SparkConf sc = null;
    public static SparkConf initSC() {
        if (sc == null) {
            sc = new SparkConf()
                    .setAppName("ParallelizeKMeans")
                    .setMaster("local");
        }
        return sc;
    }
    public double run_cost(String input, int num_cluster, int iter) {
        SparkConf conf = initSC();
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // Load and parse data
        JavaRDD<String> data = jsc.textFile(input);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(",");
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
        JavaSparkContext jsc = new JavaSparkContext(initSC());
        // Load and parse data
        JavaRDD<String> data = jsc.textFile(input);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(",");
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
        JavaRDD<Integer> res = clusters.predict(parsedData).coalesce(1);

        res.saveAsTextFile(output);
        // smallest is best
        jsc.stop();
    }
}
