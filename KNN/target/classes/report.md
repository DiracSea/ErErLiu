# CS226 Assignment 2: MapReduce

Name: Longze Su SID: 862188702

1. Which InputFormat did you use in the MapReduce program?

job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);

2. What is the input and output format of the map function?

Input: Text, DoubleWritable

Output: Text, DoubleWritable

3. What is the logic of the map function?

Read one line as point. 
The line number is input Key LongWritable:key.
The input Value is Text: value.  
Extract information from input Value and split it by "," 
The First one is Output Key, its type is String
The Second one and Third one are x and y. 
Calculate each distance between q and point_i, set it as Output Value. 

4. If a combiner function is used, what is the signature of the combiner function (input and
output) and what is its logic?

Input: Text, DoubleWritable 
Output: Text, DoubleWritable

For input data, store self-define datatype Pair<String: key, double: value>, and store them into a MaxHeap, the max size of MaxHeap is k, when element over the size, compare it with the top element of MaxHeap, if smaller, then add it to the MaxHeap. Output all the element in the MaxHeap. 


5. If a reduce function is used, what is the signature of the reduce function (input and
output) and what is its logic?

Input: Text, DoubleWritable 
Output: Text, NullWritable

The similar method as the combiner method, for input data, store self-define datatype Pair<String: key, double: value>, and store them into a MaxHeap, the max size of MaxHeap is k, when element over the size, compare it with the top element of MaxHeap, if smaller, then add it to the MaxHeap. Output all the key of element in the MaxHeap.

6. How many mappers and reducers are needed for your program?

mapper: 4, reducer: 1

7. How many records are shuffled between the mappers and reducers?

700

### Result

       File System Counters
                FILE: Number of bytes read=181767
                FILE: Number of bytes written=2858872
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=1695230884
                HDFS: Number of bytes written=1900
                HDFS: Number of read operations=53
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=12
                HDFS: Number of bytes read erasure-coded=0
        Map-Reduce Framework
                Map input records=10507403
                Map output records=10507403
                Map output bytes=283699881
                Map output materialized bytes=20324
                Input split bytes=396
                Combine input records=10507403
                Combine output records=700
                Reduce input groups=700
                Reduce shuffle bytes=20324
                Reduce input records=700
                Reduce output records=100
                Spilled Records=2000
                Shuffled Maps =4
                Failed Shuffles=0
                Merged Map outputs=4
                GC time elapsed (ms)=408
                Total committed heap usage (bytes)=1022877696
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=444949970
        File Output Format Counters
                Bytes Written=1900
