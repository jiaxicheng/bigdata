"""
https://stackoverflow.com/questions/58451588/how-extract-numbers-from-text-no-splited
    
Use regex pattern to clean out the raw data so that we have only one anchor `ABC` to 
identify the start of a potential match:

Use Accumulator to print potential ERROR message to the driver

Note:

  * in `clean2` pattern, ABCS and ABS are treated the same way. if they are different, just remove the 'S'
    and add a new alternative `ABCS(?=\d)` to the end of the pattern

        re.compile(r'\bABC(?:[/\s-]+KE|(?=\s*\d))|\bFOR\s+(?:[A-Z]+\s+)*|ABCS(?=\d)')

  * current pattern `clean1` only treats '-', '&' and whitespaces as consecutive connector, you might add 
    more characters or words like 'and', 'or', for example:

        re.compile(r'[-&\s]+|\b(?:AND|OR)\b')

  * `FOR words` is \bFOR\s+(?:[A-Z]+\s+)*, this might be adjusted based on if numbers are allowed 
    in words etc.


"""

import re
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
from pyspark import AccumulatorParam
    

"""extend AccumulatorParam class to use List as an accumulator"""
class ListAccumulatorParam(AccumulatorParam):
  def zero(self, v):
    return []
  def addInPlace(self, acc1, acc2):
    return acc1 + acc2

"""
regex patters:
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
    'clean1': re.compile(r'[-&\s]+', re.UNICODE)
  , 'clean2': re.compile(r'\bABCS?(?:[/\s-]+KE|(?=\s*\d))|\bFOR\s+(?:[A-Z]+\s+)*', re.UNICODE) 
  , 'data'  : re.compile(r'\b(\d{4,6})(?=[A-Z/]|$)', re.UNICODE) 
}   
    
""" function to find matched numbers and save them into an array"""
def find_number(s_t_r, ptns, is_debug=0):
  try:          
    arr = re.sub(ptns['clean2'], 'ABC ', re.sub(ptns['clean1'], ' ', s_t_r.upper())).split()
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
    global errors
    errors += ['ERROR:{}:\n  [{}]\n'.format(s_t_r, e)]
    #print('ERROR:{}:\n  [{}]\n'.format(s_t_r, e))
    return []


if __name__ == '__main__':

    spark = SparkSession.builder \
                        .master('local[*]') \
                        .appName('test') \
                        .getOrCreate()
    
    df = spark.read.csv('/home/xicheng/test/regex-1.txt', sep='|', header=True)
     
    errors = spark.sparkContext.accumulator([], ListAccumulatorParam())

    # defind the udf function
    udf_find_number = udf(lambda x: find_number(x, ptns), ArrayType(StringType()))

    # get the new_column
    df_new = df.withColumn('new_column', udf_find_number('column'))

    df_new.show(truncate=False)
    
    if errors.value:
      print('\n'.join(errors.value))

    # debug local testing:
    #for row in df.limit(10).toLocalIterator():
    #  print('{}:\n    {}\n'.format(row.column, find_number(row.column, ptns))) 

    spark.stop()


