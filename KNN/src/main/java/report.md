# CS226 Assignment 2: MapReduce
1. Which InputFormat did you use in the MapReduce program?


2. What is the input and output format of the map function?
Input: Text, DoubleWritable

Output: Text, DoubleWritable

3. What is the logic of the map function?

Read one line as point. 
The line number is Key LongWritable:key.
The Value is Text: value.  
Extract information from input and split it by "," 
The First one is Index, its type is String
The Second one and Third one are x and y. 
Calculate each distance between q and p_i 

1. If a combiner function is used, what is the signature of the combiner function (input and
output) and what is its logic?

5. If a reduce function is used, what is the signature of the reduce function (input and
output) and what is its logic?

6. How many mappers and reducers are needed for your program?

7. How many records are shuffled between the mappers and reducers?

### example data
```text
477926640748863489,39.9406226,32.8985482
477926639108882432,51.44588529,-0.02442135
477926640925040640,44.84020484,-0.60900226
477926640605859841,30.05152726,-95.21058362
477926640992153600,5.5807395,-0.1362842
477926640966971392,-34.9124252,-56.1828363
477926641159503873,29.33450175,48.03227653
477926640832368640,38.46251476,-121.42671808
477926639884845056,51.51388082,-0.16446591
477926641151512576,59.5599384,10.272352
477926641113763840,38.47932262,28.14915126
477926641373437953,45.66389899,-122.62442532
477926640962764800,43.0921547,2.3085233
477926641084403712,40.38318226,-79.89911708
```