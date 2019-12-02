package main;

import org.apache.spark.sql.*;

import java.io.*;

public class score {
    private static SparkSession spark = null;

    public static SparkSession initSpark() {
        if (spark == null) {
            spark = SparkSession
                    .builder()
                    .appName("TF.IDF")
                    .getOrCreate();
        }
        return spark;
    }
    public Dataset<Row> getValue(String path, String src) {
        String col = "score";
        SparkSession spark = initSpark();
        Dataset<Row> df = spark.read().json(path+"/"+src+"/COMMENTS_"+src+".json").select(col);
        df.describe().show();

        Dataset<Row> des = df
                .select(functions.mean(col).alias("mean"), functions.min(col).alias("min"),
                        functions.max(col).alias("max"), functions.stddev(col).alias("stddev"));
        des.show();
        Dataset<Row> df1 = spark.read().json(path+"/"+src+"/SUBMISSION_"+src+".json").select(col, "upvote_ratio");

        Dataset<Row> attr = des
                .withColumn("label", functions.lit(src))
                .withColumn("glo_score", functions.lit(df1.select(col).head().getLong(0)))
                .withColumn("upvote_ratio", functions.lit(df1.select("upvote_ratio").head().getDouble(0)));
        attr.show();
        return attr.select("label", "mean", "stddev", "min", "max", "glo_score", "upvote_ratio");
    }
    public static void main(String[] args) throws IOException {
        String input = args[0], output = args[1];
        single s = new single();
        score s1 = new score();
        String[] dir = s.findDir(input);


        Dataset<Row> res;
        for (String d: dir) {
            if (d.equals("movie")) break;
            res = s1.getValue(input, d);
            res.write().mode(SaveMode.Append).format("org.apache.spark.sql.json").save(output);
        }

    }

}
