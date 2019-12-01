package main;

import java.io.*;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import scala.Tuple2;


public class single {
    private static SparkSession spark = null;
/*    private static JavaSparkContext sc = null;
    public static JavaSparkContext initContext(){
        if (sc == null)
            sc = new JavaSparkContext(new SparkConf());
        return sc;
    }*/

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
        new_df2.collect();
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

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
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
    public Dataset<Row> getValue(String path, String tw) {

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
        Dataset<Row> reddit = res.filter("label = 'Reddit'").select("filtered", "features");
        Dataset<Row> twitter = res.filter("label = 'Twitter'").select("filtered", "features");

//        UDF1 dot = new UDF1<Row[], double[]>() {
//            @Override
//            public double[] call(Row[] row) throws Exception {
//                Row r = row[0], t = row[1];
//                String
//                return new double[0];
//            }
//        };

        Dataset<SimilarText> similarTextDataset = twitter.join(reddit)
                .map(r -> {
                    String label1 = r.getString(0);
                    String label2 = r.getString(2);
                    Vector fTwitter = r.getAs(1);
                    Vector fR = r.getAs(3);
                    double ddot = BLAS.dot(fTwitter.toSparse(), fR.toSparse());
                    double v1 = Vectors.norm(fTwitter.toSparse(), 2.0);
                    double v2 = Vectors.norm(fR.toSparse(), 2.0);
                    double sim = ddot / (v1*v2);

                    SimilarText similarText = new SimilarText();
                    similarText.setLabel1(label1);
                    similarText.setLabel2(label2);
                    similarText.setSimilarity(sim);
                    return similarText;
                }, Encoders.bean(SimilarText.class));
        Dataset<Row> sim = spark.createDataFrame(
                similarTextDataset.toJavaRDD(),
                SimilarText.class
        );
        sim.select("label1", "label2", "similarity").createOrReplaceTempView("tmp");
        Dataset<Row> ds = spark.sql("select label1, label2, similarity from" +
                "(select a.*, row_number() over (partition by label1 order by similarity asc) " +
                "as seqnum from tmp) a where seqnum <= 5 order by a.similarity asc");
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

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        String input = args[0], output = args[1], tw = args[2], output1 = args[3];
        single s = new single();
        String[] dir = s.findDir(input);
        // similarDataset(s.getValue(input, tw));
        Dataset<Row> res = s.getValue(input, tw);
        res.toJSON().javaRDD().repartition(1).saveAsTextFile(output);

        Dataset<Row> sim = similarDataset(res);
        res.toJSON().javaRDD().repartition(1).saveAsTextFile(output1);
/*        boolean append = true;
        boolean autoFlush = true;
        String charset = "UTF-8";
        String filePath = output;
        String tmp;*/

/*        File file = new File(filePath);
        FileOutputStream fos;
        OutputStreamWriter osw;
        BufferedWriter bw;
        PrintWriter pw;


        for (String d : dir) {
            if (d.equals("movie")) continue;
            if(!file.getParentFile().exists()) file.getParentFile().mkdirs();
            fos = new FileOutputStream(file, append);
            osw = new OutputStreamWriter(fos, charset);
            bw = new BufferedWriter(osw);
            pw = new PrintWriter(bw, autoFlush);
            pw.write(d);
            break;
        }*/
    }
}
