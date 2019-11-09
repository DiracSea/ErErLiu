package edu.ucr.cs.cs226;

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


    public void findPair(String input, String output) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
        JavaRDD<String> in = sc.textFile(input);

        // split
        JavaPairRDD<String, String> table = in
                .mapToPair(s -> new Tuple2<>(s.split("\t")[0]+s.split("\t")[4],s));
        // join
        JavaPairRDD<String, Tuple2<String, String>> pair = table
                .join(table)
                .mapToPair(s -> new Tuple2<String, Tuple2<String, String>>(s._2._1+"\t"+s._2._2, s._2))
        // filter
                .filter(new Function<Tuple2<String, Tuple2<String, String>>, Boolean>() {
                    public Boolean call(Tuple2<String, Tuple2<String, String>> tmp) throws Exception {
                        String[] tmp1 = tmp._2._1.split("\t");
                        String[] tmp2 = tmp._2._2.split("\t");
                        String host1 = tmp1[0], host2 = tmp2[0];
                        String url1 = tmp1[4], url2 = tmp2[4];
                        long time1 = Long.valueOf(tmp1[2]), time2 = Long.valueOf(tmp2[2]);

                        if ((time1 - time2 >= 0 && tmp1 != tmp2) && time1 - time2 <= 3600 && host1 == host2 && url1 == url2){
                            return true;
                        }
                        return false;
                    }
                });
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
