## Count Word Pairs ##

There are cases when people want to count the number pairs of two consecutive words. Given the words in 
an electronic text document can have word-wraps within the same paragraph, it's important to collect all 
words in the same paragraph before the Mapping task.

The expected data are all unstructured and thus using RDD APIs is the ideal way to go.

There is no direct way to read and process RDD by paragraph unless there is no line-wraps within all
paragraphs. PySpark provides different API functions to read unstructured text files into the SparkContext: 

1. The most common way sc.textFile() which read data from files line-by-line
2. Using sc.wholeTextFiles(), this loads data into a list of tuples (file_path, file_content)

Below list some code to load data and set up functions to create maps of tuples of consecutive words
((word1, word2), 1).
```
from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setAppName("Count.Word.Pairs").setMaster("spark://lexington:7077")
sc = SparkContext(conf=conf)

def asplit(x):
   z = x.lower().split()
   return [ ((z[i], z[i+1]),1) for i in range(len(z)-1) ]

```

Now the main concern is how to group lines into paragraphs, under Linux bash, this could be 
handled in one line like:  `perl -i.orig -lp00e 's/\n/ /g' filename.txt`. How to do this in
PySpark? 

### Method-1: using sc.wholeTextFiles() ###

This is ideal when there are many small files or files which can be load into memory with ease.
```
rdd1 = sc.wholeTextFiles("hdfs:///user/xicheng/projects/data/")

# get the top-5 of the pairs
rdd1.flatMap(lambda x: [ t for k in x[1].split("\n\n") for t in asplit(k)]) \
    .reduceByKey(add)                                                       \
    .top(5,key=lambda x: x[1]))

```
In the flatMap() function, file_content can be access by x[1] (lambda function).
We can split the file_content by "\n\n"(AKA. an EMPTY line) which is to separate
paragraphs, and then run the user-defined asplit() function to create the tuples 
for counting.

This is the simplest way and recommended when the file-size is manageable.


### Method-2: using sc.textFile() and rdd.reduce() function ###

When using sc.textFile(), the file_contents are read line-by-line, there is no way
using map(), flatMap etc to identify the boundaries of a paragraph. reduce() and related 
functions, on the other hand, do read two lines at once into the function,
thus can be used to identify if a paragraph ends.

**Note:** unluckily, Accumulator is write-only global variable and can't be used in 
Spark task for any programming logic.

Below is one reduce function which can convert paragraphs into list of strings
Note: paragraphs separated by EMPTY lines, partitions can be anywhere to separate the
RDD into chunks.
```
def merge_lines_in_paragraph(x, y):
    if y:
        # merging from different partitions
        if type(x) is list and type(y) is list:
            x[-1] += ' ' + y[0]
            x = x + y[1:]
        # normal in the same partition or merging from another partition
        elif type(x) is list:
            x[-1] += ' ' + y
        # merging from different partitions
        elif type(y) is list:
            y[0] = x + ' ' + y[0]
            return y
        # the same partition, first runs, can also be data merged from another partitions(if both have only one line)
        elif x:
            return [x + ' ' + y]
        # the same partition, first runs
        else:
            return ['', y]
    else:
        # the end of a partition/paragraph
        if type(x) is list:
            x.append('')
        # the same partitions, have only one item or the 2nd item is EMPTY line
        elif x:
            return [x,'']
        # consecutive EMPTY lines
        else:
            return ['']
    return x

# read data into RDD
rdd1 = sc.textFile("file:///home/hdfs/test/pyspark/word-count-1.txt", minPartitions=10)

# run the reduce task and then reload the resultset into RDD
rdd2 = sc.parallelize(rdd1.reduce(merge_lines_in_paragraph))

# do the counting tasks
rdd2.flatMap(asplit)            \
    .reduceByKey(add)           \
    .top(5, key=lambda x:x[1])

```
It's important to count on the partitions when programming PySpark. For example
if running the reduce() task with only one partition, you will never see `y` as a list in 
the above reduce function: merge_lines_in_paragraph().

The downside of this method is the lack of simplicity from the previous method and 
it needs to save intermediate result set from the reduce task.

What to do if the data set is very huge that Method-1 does not apply:

+ download the data to local file system, do a Perl one liner mentioned earlier
  and then reload the data. 

+ use Method-2 with caution, since this needs to save intermediate data, thus could be slow.


Method-2 can also be handled using a list of lists, same logic as using a list of strings.
See below 
```
def merge_lines_in_paragraph_2(x, y):
    if y:
        if type(x) is list and type(y) is list:
            x[-1].extend(y[0])
            x = x + y[1:]
        elif type(x) is list:
            x[-1].append(y)
        elif type(y) is list:
            y[0] = [x] + y[0]
            return y
        elif x:
            return [[x, y]]
        else:
            return [[], [y]]
    else:
        if type(x) is list:
            x.append([])
        elif x:
            return [[x],[]]
        else:
            return [[]]
    return x

rdd1 = sc.textFile("file:///home/hdfs/test/pyspark/word-count-1.txt", minPartitions=1)
rdd2 = sc.parallelize(rdd1.reduce(merge_lines_in_paragraph_2))
rdd2.map(lambda x: ' '.join(x)) \
    .flatMap(asplit)            \
    .reduceByKey(add)           \
    .top(5, key=lambda x:x[1])
```

### Method-3: using sc.textFile() and rdd.aggregate() function ###

pyspark provides aggregate() method which can isolate the operations based on partitions.
Thus we can define reduce() function within a partiton and between different partitions.
We will set up an initial value of reduce result to [''], this inital x must be a list.
The logic can be simplified as below:

```
# Within the same partitions, we know that y must not be a list
# since x is a list, if y is not empty, then concatenate it with
# the last element in the list x. if y is empty line, then it should be
# add to x as a standalone element(to signal a new paragraph)
def merge_within_partition(x, y):
    if y:
        x[-1] += ' ' + y
    else: 
        x.append('')
    return x

# merge is now between list and list
# the last element in x to concatenate with the first element in y
# then extend the rest of y to x
def merge_between_partitions(x,y):
    x[-1] += ' ' + y[0]
    if len(y) > 1: 
        x = x + y[1:]
    return x

rdd1 = sc.textFile("file:///home/hdfs/test/pyspark/word-count-1.txt", minPartitions=10)
rdd1.aggregate([''], merge_within_partition, merge_between_partitions)

```

### Method-4: Using sc.textFile() and rdd.fold() function ###

The difference between a fold() and reduce() is that fold() provides
an opportunity to setup an initial value, thus in func(x, y), you dont
need to worry about the data type of `x`, it will always be a list
after we setup the zeroValue to ['']

```
def fold_lines_in_paragraph(x, y):
    if y:
        # merging from a different partition
        if type(y) is list:
            x[-1] += ' ' + y[0]
            x = x + y[1:]
        # in the same partition
        else:
            x[-1] += ' ' + y
    # same partition, but y is empty line
    else:
        x.append('')
    return x
rdd1 = sc.textFile("file:///home/hdfs/test/pyspark/word-count-1.txt", minPartitions=10)
rdd1.fold([''], fold_lines_in_paragraph)

```

### Method-5: Using newAPIHadoopFile and specify delimiter ###

Method 2,3,4 all have issues when processing large files, since these reduce-like methods (
reduce, aggregate and fold) are all actions which need return data to driver and then reload
them into RDD using method like sc.parallelize(). this posts big overhead on both memory and network 
bandwidth. 

Using foldByKey() might be a solution, but since it invokes a shuffling and thus
might potentially change the order of lines (even if we use a constant Key). the reduce-like
method still need to collect all data into one list. this will eventually raise memory issue 
when the final resultset is huge.

A better solution is to use newAPIHadoopFile() to load datafile, this method provides a way
to specify the delimiter, in our case it can be '\n\n'. and then process the data like the Method-1
but without loading the whole file once at a time. 

```
rdd = sc.newAPIHadoopFile(
    '/path/to/file', 
    'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'org.apache.hadoop.io.Text', 
    conf={'textinputformat.record.delimiter': '\n\n'}
)

rdd.flatMap(lambda x: [t for t in asplit(x[1])]) \
   .reduceByKey(add) \
   .top(5,key=lambda x: x[1])

```

Further reading and References: 
+ [n-gram](https://en.wikipedia.org/wiki/N-gram)
+ [Create spark data structure from multiline record](https://stackoverflow.com/questions/31227363/creating-spark-data-structure-from-multiline-record)
