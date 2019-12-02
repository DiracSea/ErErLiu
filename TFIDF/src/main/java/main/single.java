package main;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;

import java.io.*;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;


public class single {
    private static SparkSession spark = null;
    private static JavaSparkContext sc = null;
    public static JavaSparkContext initContext(){
        if (sc == null)
            sc = new JavaSparkContext(initSpark().sparkContext());
        return sc;
    }

    public static SparkSession initSpark() {
        if (spark == null) {
            spark = SparkSession
                    .builder()
                    .appName("TF.IDF")
                    .getOrCreate();
        }
        return spark;
    }

    public static Dataset<Row> initReddit(String path, String src) {
        SparkSession spark = initSpark();

        Dataset<Row> df = spark.read()
                .json(path+"/"+src+"/COMMENTS_"+src+".json")
                .filter("score > 10")
                .select("body").limit(1);
        Dataset<Row> new_df = df
                .withColumn("body", functions.regexp_replace(df.col("body"),"[^a-zA-Z.'?!]+"," "));
        Dataset<Row> new_df1 = new_df
                .withColumn("body", functions.trim(new_df.col("body")));

        Dataset<Row> new_df2 = new_df1
                .withColumn("body", functions.regexp_replace(new_df1.col("body"), "\\s+", " "))
                .filter("body != '\\s+deleted'")
                .filter("body != 'deleted'")
                .filter("body != ''");
        new_df2.show(5);

        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("body")
                .setOutputCol("token");
        Dataset<Row> wordsData = tokenizer.transform(new_df2);

        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("token")
                .setOutputCol("filtered");
        Dataset<Row> wordFiltered = remover
                .transform(wordsData)
                // .filter("filtered != null")
                .withColumn("label", functions.lit("Reddit"));
        wordFiltered.show(5);
        return wordFiltered.select("label", "filtered");
    }

/*    public Tuple2<String, String[]> tw(String line) {
        String[] str = line
*//*                .replace(" ;['",";")
                .replace("']","")
                .replace("', '", ",")*//*
                .split(";");
        Tuple2<String, String[]> res = new Tuple2<String, String[]>(
                str[0].toString(), str[1].split(",")
        );
        return res;
    }*/

    public static class TW implements Serializable {
        private String label;
        private String body;

        public String getBody() {
            return body;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public void setBody(String body) {
            this.body = body;
        }
    }

    public static Dataset<Row> initTwitter(String path) {
        SparkSession spark = initSpark();

        JavaSparkContext sc = initContext();
        JavaRDD<String> in = sc.textFile(path);

        JavaRDD<TW> table = in
                .map(line -> {
                    String[] parts = line.split(";");
                    TW tw = new TW();
                    tw.setLabel(parts[0].trim());
                    tw.setBody(parts[1]
                            .replaceAll("\\['", "")
                            .replaceAll("', '", " ")
                            .replaceAll("']",""));
                    return tw;
                });

        Dataset<Row> twDF = spark.createDataFrame(table, TW.class).limit(5);
        twDF.show(5);

        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("body")
                .setOutputCol("token");
        Dataset<Row> wordsData = tokenizer.transform(twDF);

        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("token")
                .setOutputCol("filtered");
        Dataset<Row> wordFiltered = remover
                .transform(wordsData);
                // .filter("filtered != null");
        wordFiltered.show(5);
        return wordFiltered.select("label", "filtered");
    }
    public static Dataset<Row> getValue(String path, String tw) {

        SparkSession spark = initSpark();

        // final HashingTF hTF = new HashingTF();
		/*
		 	mllib
			spark
		 */
        single s = new single();
        Dataset<Row> twitter = initTwitter(tw);
        Dataset<Row> reddit;

        String[] dir = s.findDir(path);
        int i = 0;
        for (String d : dir) {
            i += 1;
            if (d.equals("movie") || i > 10) break;
            reddit = initReddit(path, d);

            twitter = twitter.union(reddit);
        }
        Dataset<Row> df = twitter;

/*        CountVectorizerModel cv = new CountVectorizer()
                .setInputCol("filtered")
                .setOutputCol("rawFeatures")
                .setVocabSize(50000)
                .setMinDF(2)
                .fit(wordFiltered);
        Dataset<Row> featurizedData = cv.transform(wordFiltered);
        featurizedData.show(5);*/

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

    public static class SimilarText implements Serializable, Comparable<SimilarText> {
        private String label1;
        private String label2;
        private double similarity;

        public String getLabel1() {
            return label1;
        }

        public String getLabel2() {
            return label2;
        }

        public double getSimilarity() {
            return similarity;
        }

        public void setLabel1(String label1) {
            this.label1 = label1;
        }

        public void setLabel2(String label2) {
            this.label2 = label2;
        }

        public void setSimilarity(double similarity) {
            this.similarity = similarity;
        }

        @Override
        public int compareTo(SimilarText similarText) {
            SimilarText s = similarText;

            if (this.similarity > s.similarity)
                return 1;
            else if (this.similarity < s.similarity)
                return -1;
            else {
                return 0;
            }
        }
    }
    public static Dataset<Row> similarDataset (Dataset<Row> res) {
        SparkSession spark = initSpark();
        spark.conf().set("spark.sql.crossJoin.enabled", "true");
        Dataset<Row> reddit = res.filter("label = 'Reddit'").select("label", "features");
        Dataset<Row> twitter = res.filter("label = 'Twitter'").select("label", "features");

//        UDF1 dot = new UDF1<Row[], double[]>() {
//            @Override
//            public double[] call(Row[] row) throws Exception {
//                Row r = row[0], t = row[1];
//                String
//                return new double[0];
//            }
//        };
        Dataset<Row> jj = twitter.join(reddit);
        jj.show(10);

        JavaRDD<SimilarText> similarTextDataset = jj.toJavaRDD()
                .map(r -> {
                        String label1 = r.getList(0).get(0).toString();
                        System.out.println(label1);
                        String label2 = r.getList(2).get(0).toString();
                        System.out.println(label2);
                        Vector fTwitter = r.getAs(1);
                        System.out.println(fTwitter.toString());
                        Vector fR = r.getAs(3);
                        System.out.println(fR.toString());
                        double ddot = BLAS.dot(fTwitter.toSparse(), fR.toSparse());
                        double v1 = Vectors.norm(fTwitter.toSparse(), 2.0);
                        double v2 = Vectors.norm(fR.toSparse(), 2.0);
                        double sim = ddot / (v1 * v2);
                        System.out.println(sim);

                        SimilarText similarText = new SimilarText();
                        similarText.setLabel1(label1);
                        similarText.setLabel2(label2);
                        similarText.setSimilarity(sim);
                        return similarText;
                });

        Dataset<Row> sim = spark.createDataFrame(
                similarTextDataset,
                SimilarText.class
        );
        sim.show(10);
        // sim.select("label1", "label2", "similarity").createOrReplaceTempView("tmp");
/*        .withColumn("rank",
                functions.rank().over(Window.partitionBy("label1").orderBy("similarity")))*/
        Dataset<Row> ds1 = sim
                .groupBy(col("label1").alias("label"))
                .agg(max("similarity").alias("max")
                        , min("similarity").alias("min")
                        , avg("similarity").alias("avg")
                        , stddev("similarity").alias("dev"));
        Dataset<Row> ds2 = sim
                .groupBy(col("label1").alias("label"))
                .agg(max("similarity").alias("max")
                        , min("similarity").alias("min")
                        , avg("similarity").alias("avg")
                        , stddev("similarity").alias("dev"));

        Dataset<Row> ds = ds1.select("label", "max", "min", "avg", "dev")
                .union(ds2.select("label", "max", "min", "avg", "dev"));
        ds.show(5);
        return ds;
    }

    public String[] findDir (String path) {
        File file = new File(path);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });
        System.out.println(Arrays.toString(directories));
        return directories;
    }

    public static void main(String[] args) throws IOException {
        String input = args[0], output = args[1], tw = args[2], output1 = args[3];


        // similarDataset(getValue(input, tw));

        Dataset<Row> init = getValue(input, tw);
        Dataset<Row> tmp = init;

        JavaRDD<String> res = init.toJSON().toJavaRDD();

        // System.out.println(res.toString());
        res.saveAsTextFile(output);

        JavaRDD<String> res1 = similarDataset(tmp).toJSON().toJavaRDD();

        res1.saveAsTextFile(output1);

    }
}
