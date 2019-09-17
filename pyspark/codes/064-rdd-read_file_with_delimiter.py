#!/bin/env python
"""Use newAPIHadoopFile() to create RDD with multi-lines elements

Notes:
* configure textinputformat.record.delimiter to split related text blocks into the same RDD element, this 
  delimiter must be plain-text, no regex is supported
* In this example, we use '\n<Q' as the delimiter, if the text block can be split by other tags, i.e. '\n<P'
  then you might have to setup your own Hadoop TextInputFormat class
* On the other hand, if the text blocks are split by '\n<Q', but there are optional whitespaces between '\n'
  and '<Q', then it can be resolved by using a flatMap() function, in this example:

      df = spark.sparkContext.newAPIHadoopFile(
        'file:///home/hdfs/test/pyspark/delimiter-2.txt',
        'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
        'org.apache.hadoop.io.LongWritable',
        'org.apache.hadoop.io.Text',
        conf={'textinputformat.record.delimiter': '\n<Q'}
      ).flatMap(lambda x: [ parse_rdd_element(e, ctrl_args) for e in re.split(r'\n\s+<Q', x[1]) if e ]) \
       .filter(bool) \
       .toDF()

Below sample data, each text block starts with a `<Q` tag, i.e. <Q31>, <Q25>, we will need to retrieve the
following information:

    +-------+--------------+------------+----------+
    |Item_Id|quantityAmount|quantityUnit|      rank|
    +-------+--------------+------------+----------+
    |    Q31|         24954|       Meter|  BestRank|
    |    Q25|           582|   Kilometer|NormalRank|
    +-------+--------------+------------+----------+
 
To run this program, create a file `/path/to/sample.txt` with the content shown in `Sample data` section,
and then run: (note, in this sample, my filepath is 'file:///home/xicheng/delimiter-2.txt')

    spark-submit 064-rdd-read_file_with_delimiter.py

Sample data:
---
<Q31> <prop/P1082> <Pointer_Q31-87RF> .
<Pointer_Q31-87RF> <rank> <BestRank> .
<Pointer_Q31-87RF> <prop/Pointer_P1082> "+24954"^^<2001/XMLSchema#decimal> .
<Pointer_Q31-87RF> <prop/Pointer_value/P1082> <value/cebcf9> .
<value/cebcf9> <syntax-ns#type> <QuantityValue> .
<value/cebcf9> <quantityAmount> 24954
<value/cebcf9> <quantityUnit> <Meter> .
<Q25> <prop/P1082> <Pointer_Q25-8E6C> .
<Pointer_Q25-8E6C> <rank> <NormalRank> .
<Pointer_Q25-8E6C> <prop/Pointer_P1082> "+24954"
<Pointer_Q25-8E6C> <prop/Pointer_value/P1082> <value/cebcf9> .
<value/cebcf9> <syntax-ns#type> <QuantityValue> .
<value/cebcf9> <quantityAmount> "582" .
<value/cebcf9> <quantityUnit> <Kilometer> .

References:
[1] http://spark.apache.org/docs/2.4.0/api/python/pyspark.html#pyspark.SparkContext.newAPIHadoopFile
[2] https://stackoverflow.com/questions/31227363/creating-spark-data-structure-from-multiline-record
[3] https://stackoverflow.com/questions/57499828/how-to-match-extract-multi-line-pattern-from-file-in-pysark
"""

from pyspark.sql import SparkSession, Row
import re

"""this ctrl_args includes all field names required to be parsed into dataframe
and two pre-compiled regex patterns
"""
ctrl_args = { 
    'columns': ['Item_Id', 'quantityAmount', 'quantityUnit', 'rank'],
    'patterns': {
        'quantityAmount': re.compile(r'^quantityAmount>\D*(\d+)'),
        'Item_Id': re.compile(r'^(?:<Q)?(\d+)')
    }
}

"""Function to parse the require fields from the RDD element
1. split the rdd element into lines by split('\n')
2. then using '> <' to split line into a list `y`
3. we can then using string and regex operations to find rank, quantityUnit from y[1], y[2]
   , `quantityAmount` from y[1] and `Item_Id` from y[0]
4. save the data into Row object, for any missing field, set the value to None
"""
def parse_rdd_element(x, kargs):
    try: 
        row = {}
        for e in x.split('\n'):
            y = e.split('> <')
            if len(y) < 2: 
                continue
            if y[1] in ['rank', 'quantityUnit']:
                row[y[1]] = y[2].split(">")[0]
            else:
                m = re.match(kargs['patterns']['quantityAmount'], y[1])
                if m: 
                    row['quantityAmount'] = m.group(1)
                    continue
                m = re.match(kargs['patterns']['Item_Id'], y[0])
                if m:
                    row['Item_Id'] = 'Q' + m.group(1)
        # if row is not EMPTY, set None to missing field
        return Row(**dict([ (k, row[k]) if k in row else (k, None) for k in kargs['columns']])) if row else None
    except:
        return None

if __name__ == "__main__":

    # set up Spark session
    spark = SparkSession.builder                   \
                        .appName("pyspark-test")   \
                        .getOrCreate()

    # create RDD using newAPIHadoopFile()
    rdd = spark.sparkContext.newAPIHadoopFile(
        'file:///home/xicheng/delimiter-2.txt',
        'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
        'org.apache.hadoop.io.LongWritable',
        'org.apache.hadoop.io.Text',
        conf={'textinputformat.record.delimiter': '\n<Q'}
    )
    
    # create dataframe from parsed Row object
    df = rdd.map(lambda x: parse_rdd_element(x[1], ctrl_args)).filter(bool).toDF()

    # print resulting dataframe
    df.show()
    
    spark.stop()
