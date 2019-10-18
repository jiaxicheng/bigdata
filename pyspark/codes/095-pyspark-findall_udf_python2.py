"""
https://stackoverflow.com/questions/58451588/how-extract-numbers-from-text-no-splited
    
Use regex pattern to clean out the raw data so that we have only one anchor `ABC` to 
identify the start of a potential match:

Use Accumulator to print potential ERROR message to the driver


Note:

  * in `clean2` pattern, ABCS and ABS are treated the same way. if they are different, just remove the 'S'
    and add a new alternative `ABCS(?=\d)` to the end of the pattern

        re.compile(ur'\bABC(?:[/\s-]+KE|(?=\s*\d))|\bFOR\s+(?:[A-Z]+\s+)*|ABCS(?=\d)', re.UNICODE)

  * current pattern `clean1` only treats '-', '&' and whitespaces as consecutive connector, you might add 
    more characters or words like 'and', 'or', for example:

        re.compile(ur'[-&\s]+|\b(?:AND|OR)\b', re.UNICODE)

  * `FOR words` is \bFOR\s+(?:[A-Z]+\s+)*, this might be adjusted based on if numbers are allowed 
    in words etc.

  * the print() in except block only works in local mode. to print error message to driver, you can
    extend AccumulatorParam to add list information for debugging.

Python 2 has issue with unicode. add following lines to change default encoding of Python-2
ref: https://stackoverflow.com/questions/2276200/changing-default-encoding-of-python

    import sys
    # sys.setdefaultencoding() does not exist, here!
    reload(sys)  # Reload does the trick!
    sys.setdefaultencoding('UTF8')

"""

import re
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
from pyspark import AccumulatorParam
import sys

"""extend AccumulatorParam class to use List as an accumulator"""
class ListAccumulatorParam(AccumulatorParam):
  def zero(self, v):
    return []
  def addInPlace(self, acc1, acc2):
    return acc1 + acc2


"""RegEx patterns:
---
  + clean1: use `[-&\s]+` to convert consecutive '&', '-' and whitespaces to ' ', they 
            are used to connect a chain of numbers
            example: `ABC - KE`  -->  `ABC KE`
                     `103035 - 101926 - 105484` -> `103035 101926 105484`
                     `111553/L00847 & 111558/L00895` -> `111553/L00847 111558/L00895`
    
  + clean2: convert text matching the following three sub-patterns into 'ABC '
    ---
    + ABCS?(?:[/\s]+KE|(?=\s*\d))
      + ABC followed by an optional `S`
        + followed by optional whitespaces and then at least one digit  --> (?=\s*\d) 
            example: ABC898456 -> `ABC 898456`
        + or followed by at least one slash or whitespace and then `KE` --> `[/\s]+KE`  
            example: `ABC/KE 110330/L63868` to `ABC  110330/L63868`
    + \bFOR\s+(?:[A-Z]+\s+)*
      + `FOR` words
            example: `FOR DEF HJK 12345` -> `ABC 12345` 
    
  + data: \b(\d{4,6})(?=[A-Z/]|$)
    + regex to match actual numbers: 4-g digits followed by [A-Z/] or end_of_string   

"""
ptns = { 
    'clean1': re.compile(ur'[-&\s]+', re.UNICODE)
  , 'clean2': re.compile(ur'\bABCS?(?:[/\s-]+KE|(?=\s*\d))|\bFOR\s+(?:[A-Z]+\s+)*', re.UNICODE) 
  , 'data'  : re.compile(ur'\b(\d{4,6})(?=[A-Z/]|$)', re.UNICODE) 
}   
    
""" function to find matched numbers and save them into an array"""
def find_number(s_t_r, ptns, is_debug=0):
  try:          
    arr = re.sub(ptns['clean2'], u'ABC ', re.sub(ptns['clean1'], ' ', s_t_r.upper())).split()
    if is_debug: return arr
    f = 0; new_arr = [];
    for x in arr:          
      if x == 'ABC':
        f = 1              
      elif f:            
        new = re.findall(ptns['data'], x)     
        if new:              
          new_arr.extend(new)                
        else:              
          f = 0                
    return new_arr
  except Exception as e:
   # global errors
   # errors += ['ERROR:{}:\n  [{}]\n'.format(s_t_r, e)]
    print(u'ERROR:{}:\n  [{}]\n'.format(s_t_r, e))
    return []


if __name__ == '__main__':

    # for Python-2 only
    reload(sys)
    sys.setdefaultencoding('UTF8')

    spark = SparkSession.builder \
                        .master('local[*]') \
                        .appName('test') \
                        .getOrCreate()
    
    df = spark.read.csv('file:///home/xicheng/test/regex-1.txt', sep='|', header=True, encoding='utf8')
     
    errors = spark.sparkContext.accumulator([], ListAccumulatorParam())

    # defind the udf function
    udf_find_number = udf(lambda x: find_number(x, ptns), ArrayType(StringType()))

    # get the new_column
    df_new = df.withColumn('new_column', udf_find_number('column'))

    df_new.show(truncate=False)
    
    if errors.value:
      print('\n'.join(errors.value))

    # debug local testing:
    #for row in df.limit(20).toLocalIterator():
    #  print('{}:\n    {}\n'.format(row.column, find_number(row.column, ptns))) 

    spark.stop()


