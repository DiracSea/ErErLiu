package main;

import java.io.*;
import java.util.Arrays;

import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.*;


public class single {
    public Dataset<Row> initReddit(String path, String src) {
        SparkSession spark = SparkSession
                .builder()
                .appName("TF.IDF")
                // .config("")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .json(path+"/"+src+"/COMMENTS_"+src+".json")
                .select("body");
        Dataset<Row> new_df = df
                .withColumn("body", functions.regexp_replace(df.col("body"),"[^a-zA-Z.']+"," "));
        Dataset<Row> new_df1 = new_df
                .withColumn("body", functions.trim(new_df.col("body")));

        Dataset<Row> new_df2 = new_df1
                .withColumn("body", functions.regexp_replace(new_df1.col("body"), "\\s+", " "))
                .filter("body != ' deleted'")
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
                .filter("filtered != ''")
                .withColumn("label", functions.lit(src));
        wordFiltered.show(5);
        return wordFiltered;
    }

    public Dataset<Row> initTwitter(String path) {
        return null;
    }
    public Dataset<Row> getValue(String path, String src, String input) {

        SparkSession spark = SparkSession
                .builder()
                .appName("TF.IDF")
                // .config("")
                .getOrCreate();

        // final HashingTF hTF = new HashingTF();

		/*
		 	mllib
			spark
		 */
        single s = new single();
        String[] dir = s.findDir(input);
        for (String d : dir) {

        }



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

        Dataset<Row> featurizedData = hashingTF.transform(null);
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
        return rescaledData.select("filtered", "");
    }

    public String[] findDir(String path) {
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
        String input = args[0], output = args[1];
        single s = new single();
        String[] dir = s.findDir(input);

        boolean append = true;
        boolean autoFlush = true;
        String charset = "UTF-8";
        String filePath = output;
        String tmp;

        File file = new File(filePath);
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
        }
    }
}
