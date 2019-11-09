package edu.ucr.cs.cs226;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;




public class Spark {
    public Tuple2<Integer, Tuple2<Integer, Integer>> map2Tuple(String line) {
        String[] str = line.split("\t");
        Tuple2<Integer, Tuple2<Integer, Integer>> res = new Tuple2<Integer, Tuple2<Integer, Integer>>(
                Integer.parseInt(str[5]), new Tuple2<Integer, Integer>(
                Integer.parseInt(str[6]), 1));
        return res;
    }
    public void findAve(String input, String output) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
        JavaRDD<String> in = sc.textFile(input);

        //calculate average
        JavaPairRDD<String, Double> averagePair = in
                //.map(this::map2Tuple)
                .map(line -> line.split("\t"))
                .mapToPair(s -> new Tuple2<>(s[5],Integer.parseInt(s[6])))
                //.combineByKey(bytes -> new Tuple2<>(bytes, 1),
                //        (comb, new_bytes) -> new Tuple2<>(comb._1 + new_bytes, comb._2 + 1),
                //        (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
                .mapValues(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2)) // x 1st element, y 2nd element
                .mapValues(x -> Double.valueOf(x._1)/x._2);
        
        JavaRDD<String> ave = averagePair
                .map(data -> "Code "+data._1 + ", average number of bytes = " + data._2);

        //print averageByKey ("Code "+data._1() + ", average number of bytes = " + data._2());
        ave.saveAsTextFile(output);

        //stop sc
        sc.stop();
        sc.close();
    }

    public class URL implements Serializable {
        private String url;
        private String host; 
        private long time; 

        public URL (String s1, String s2, long t) {
            url = s1; 
            host = s2; 
            time = t; 
        }
        public String U() {
            return url;
        }
        public String H() {
            return host; 
        }
        public long T() {
            return time; 
        }
    }
    public void findPair(String input, String output) {
        // host 0; - 1; timestamp 2; ins 3; url 4; code 5; bytes 6
        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
        JavaRDD<String> in = sc.textFile(input);

        // split
        JavaPairRDD<String, String> table = in
                .mapToPair(s -> new Tuple2<>(s.split("\t")[0]+s.split("\t")[4],s));
        // join
        JavaPairRDD<String, Tuple2<String[], String[]>> pair = table
                .join(table)
                .mapToPair(s -> new Tuple2<String, Tuple2<String, String>>(s._2._1+"\t"+s._2._2, s._2))
        // serializabale
                .mapValues(s -> new Tuple2<>(s._1.split("\t"),s._2.split("\t")))
                // .mapValues(s -> new Tuple2<>(new URL(s._1[0],s._1[4],Long.valueOf(s._1[2])), new URL(s._2[0],s._2[4],Long.valueOf(s._2[2]))))
        // filter
                .filter(
                    // new Function<Tuple2<String, Tuple2<String[], String[]>>, Boolean>() {
                    // @Override
                    // public Boolean call(Tuple2<String, Tuple2<String[], String[]>> tmp) throws Exception {
                        // 0 < t1 - t2 <= 3600; h1 == h2; u1 == u2
                        tmp -> ((Long.valueOf(tmp._2._1[2]) - Long.valueOf(tmp._2._2[2]) > 0 && 
                        Long.valueOf(tmp._2._1[2]) - Long.valueOf(tmp._2._2[2]) <= 3600) && 
                        (tmp._2._1[0] == tmp._2._2[0] && tmp._2._1[4] == tmp._2._2[4]))
                        );
        JavaRDD<String> outFile = pair
                .map(s -> s._1);
        //print
        outFile.saveAsTextFile(output);

        //stop sc
        sc.stop();
        sc.close();
    }

    public static void main(String[] args) {
        // host 0; - 1; timestamp 2; ins 3; url 4; code 5; bytes 6
        /*
        * args[0] = "/data/input/nasa.tsv"
        * args[1] = output1
        * args[2] = output2
        */
        Spark s = new Spark();
        s.findAve(args[0], args[1]);
        s.findPair(args[0], args[1]);

    }
}
