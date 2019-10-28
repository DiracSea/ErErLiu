package edu.ucr.cs.cs226.lsu018;

// hadoop
import jdk.nashorn.internal.runtime.regexp.joni.exception.InternalException;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// java
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class KNN {

    public static class MapKNN extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
        /*
        * <key, value>
        * input: <id, pair>
        * output: <id, distance>
        * */

        private double x, y;
        private LongWritable id = new LongWritable();
        private DoubleWritable dist = new DoubleWritable();

//        @Override
//        public void setup(Context context) throws IOException{
//            Path pt=new Path(src);//Location of file in HDFS
//            FileSystem fs = FileSystem.get(new Configuration());
//            FSDataInputStream in = fs.open(pt);
//            BufferedReader br=new BufferedReader(new InputStreamReader(in));
//            String line;
//            line=br.readLine();
//            while (line != null){
//                // System.out.println(line);
//                line=br.readLine();
//            }
//        }
        @Override
        public void map(LongWritable key, Text value, Context context) // key, value
                throws IOException, InterruptedException {
//            FileSplit fileSplit = (FileSplit) context.getInputSplit();
//            String fileName = fileSplit.getPath().getName();
            Configuration conf = context.getConfiguration();
            x = Double.valueOf(conf.get("x"));
            y = Double.valueOf(conf.get("y"));

            String[] point = value.toString().split(",");
            // System.out.println(point); 
            dist.set(Euclidean_Dist(point[1]+","+point[2]));
            id.set(Long.valueOf(point[0]));

            context.write(id, dist);
        }

        public double Euclidean_Dist(String point) {
            String[] ab = point.split(",");
            double a = Double.valueOf(ab[0]);
            double b = Double.valueOf(ab[1]);
            return Math.sqrt((a-x)*(a-x) + (b-y)*(b-y));
        }
    }

    public static class ReduceKNN extends Reducer<LongWritable, DoubleWritable, LongWritable, NullWritable> {
        /*
         * <key, value>
         * input: <id, distance>
         * output: <id, null>
         * */
        private int k;
        private LongWritable id = new LongWritable();
        // private ArrayList<Pair> list = new ArrayList<Pair>();
        private MaxHeap<Pair> maxHeap = null;

//        @Override
//        public void setup(Context context) throws IOException, InternalException {
//
//        }

        @Override
        public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = Integer.valueOf(conf.get("k"));

            double sum = 0.0;
            int num = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
                num += 1;
            }
            long k = key.get();
            double v = sum/num;
            Pair p = new Pair(k, v);

            maxHeap.add(p);
            System.out.println(maxHeap.findMax().getValue()); 
            if (maxHeap.size() == k) {
                maxHeap.popMax();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
//            Configuration conf = context.getConfiguration();
//            k = Integer.valueOf(conf.get("k"));
            for (Object o : maxHeap) {
                Pair p = (Pair) o;
                id.set(p.getKey());
                context.write(id, null);
            }

        }

    }

    public static class Pair implements Comparable<Pair>{
        // Key, Dist
        private long key;
        private double value;

        public Pair(long key, double value) {
            this.key=key;
            this.value=value;
        }

        public double getValue() {
            return value;
        }

        public long getKey() {
            return key;
        }

        @Override
        public int compareTo(Pair o) {

            Pair p = o;

            if (this.value > p.value)
                return 1;
            else if (this.value < p.value)
                return -1;
            else
                return 0;
        }
    }


    public static class MaxHeap<Pair extends Comparable<Pair>> implements Iterable{
        private ArrayList<Pair> data;

        public MaxHeap(int capacity) {
            data = new ArrayList<Pair>(capacity);
        }

        public MaxHeap(Pair[] arr) {
            // Construction function
            data = new ArrayList<Pair>(Arrays.asList(arr));
            for (int i = parent(arr.length - 1); i >= 0; i--) {
                siftDown(i);
            }
        }

        // iterator
        public Iterator iterator() {
            return new PairIterator();
        }

        private class PairIterator implements Iterator {
            private int idx = 0;

            public boolean hasNext() {
                return idx != data.size();
            }

            public Pair next() {
                return data.get(idx++);
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        }

        // return element of heap
        public int size() {
            return data.size();
        }

        public boolean isEmpty() {
            return data.size() == 0;
        }

        private int parent (int idx) {
            if (idx == 0) {
                throw new IllegalArgumentException("Index zero doesn't have parent");
            }
            return (idx - 1)/2;
        }

        private int leftChild (int idx) {
            return idx*2 + 1;
        }

        private int rightChild (int idx) {
            return idx*2 + 2;
        }

        public void add (Pair e) {
            data.add(e);
            siftUp(data.size() - 1);
        }

        public void siftUp (int k) {
            while (k > 0 && data.get(parent(k)).compareTo(data.get(k)) < 0) {
                swap(k, parent(k));
                k = parent(k);
            }
        }

        public Pair findMax() {
            if (data.size() == 0) {
                throw new IllegalArgumentException("Can not find max when heap is empty");
            }
            return data.get(0);
        }

        private void siftDown (int k) {
            while (leftChild(k) < data.size()) {
                int j = leftChild(k);
                if (j + 1 < data.size() && data.get(j + 1).compareTo(data.get(j)) > 0) {
                    j = rightChild(k);
                }

                if (data.get(k).compareTo(data.get(j)) >= 0) {
                    break;
                }

                swap(k, j);
                k = j;
            }
        }
        public Pair replace(Pair e) {
            Pair ret = findMax();
            data.set(0, e);
            siftDown(0);
            return ret;
        }

        public Pair popMax () {
            Pair ret = data.get(0);
            data.remove(0);
            siftDown(0);
            return ret;
        }

        private void swap (int i, int j) {
            Pair tmp = data.get(i);
            data.set(i, data.get(j));
            data.set(j, tmp);
        }
    }


    public static void main(String[] args) throws Exception {
        /*
        * args[]
        * String: inputPath
        * int: k
        * double: x_q
        * double: y_q
        * String: outputPath
        * */
//        MapKNN m = new MapKNN();
//        m.setX(Double.valueOf(args[2]));
//        m.setY(Double.valueOf(args[3]));
//
//        ReduceKNN r = new ReduceKNN();
//        r.setK(Integer.valueOf(args[1]));

        Configuration conf = new Configuration();

        conf.set("k", args[1]);
        conf.set("x", args[2]);
        conf.set("y", args[3]);

        Job job = Job.getInstance(conf, "Read a File");
        FileSystem fs = FileSystem.get(conf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        if (fs.exists(new Path(args[4])))
            fs.delete(new Path(args[4]), true);
        job.setMapperClass(KNN.MapKNN.class);
        job.setReducerClass(KNN.ReduceKNN.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /*
        * <key, value>
        * input: <id, pair>
        * output: <id, distance>
        * */
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        /*
         * <key, value>
         * input: <id, distance>
         * output: <id, null>
         * */
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        job.setJarByClass(KNN.class);
        job.waitForCompletion(true);
    }
}
