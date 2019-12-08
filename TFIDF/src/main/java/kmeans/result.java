package kmeans;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class result {
    public static void main(String[] args) {
        String input = args[0], cluster = args[1], output = args[2], output1 = args[3];
        int c = Integer.parseInt(cluster), best_i = 0;
        double res, best = Double.POSITIVE_INFINITY;
        kmeans k = new kmeans();

        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new File(output));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder builder = new StringBuilder();
//        String ColumnNamesList = "cluster number,iterate number,WSSSE";
//// No need give the headers Like: id, Name on builder.append
//        builder.append(ColumnNamesList + "\n");
        for (int i = 1; i < c+1; i++) {
            res = k.run_cost(input, i, 50);
            if (res < best) {
                best = res;
                best_i = i;
            builder.append(String.valueOf(i)+","+res+"\n");
            pw.write(builder.toString());
            }
        }


        pw.close();
        System.out.println("done!");

        k.run_kmeans(input, best_i, 50, output1);

    }
}

