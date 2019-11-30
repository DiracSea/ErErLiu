package feature;

import TFIDF.single;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.*;

public class score {
    public String getValue(String path, String src) {
        String col = "score";
        SparkSession spark = SparkSession
                .builder()
                .appName("TF.IDF")
                // .config("")
                .getOrCreate();
        Dataset<Row> df = spark.read().json(path+"/"+src+"/COMMENTS_"+src+".json").select(col);
        Dataset<Row> des = df
                .select(functions.mean(col).alias("mean"), functions.min(col).alias("min"),
                        functions.max(col).alias("max"), functions.stddev(col).alias("stddev"));
        df.withColumn("name", functions.lit(src));
        String res = df.describe().toJSON().toString();
        return res;
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
            if(!file.getParentFile().exists()) file.getParentFile().mkdirs();
            fos = new FileOutputStream(file, append);
            osw = new OutputStreamWriter(fos, charset);
            bw = new BufferedWriter(osw);
            pw = new PrintWriter(bw, autoFlush);
            tmp = s.getValue(input, d);
            pw.write(tmp);
        }
    }

}
