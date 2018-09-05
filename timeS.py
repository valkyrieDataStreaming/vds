from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

from pyspark.streaming import StreamingContext

sc = SparkContext('local[2]', 'timeS')
ssc = StreamingContext(sc, 1)

spark = SparkSession(sc)

data_file = ssc.socketTextStream("localhost", 9999)

from pyspark.sql.window import Window
from pyspark.sql import functions as func
import matplotlib.pyplot as plt
import pandas as pd
from pandas import Series
import pyspark.sql as sparksql
import numpy as np
import time 
from pprint import pprint
days = lambda i: i * 86400

data_file = "./export.csv"
raw_data = sc.textFile(data_file)
csv_data = raw_data.map(lambda x: x.split(","))
#csv_data.toDF().show()
head_rows = csv_data.take(5)
pprint(head_rows[0])

df1 = spark.read.csv('export.csv', header='true')
#schema = StructType([StructField(str(i), StringType(), True) for i in range(4)])
#df1 = spark.createDataFrame(csv_data, schema)
df1.show()


#pd.to_numeric(df1.select('fruits_and_vegetables'), errors='coerce')



fruits_data = df1.select(df1['fruits_and_vegetables'].astype('float'))
fruits_data = fruits_data.select('fruits_and_vegetables')
fruits_data.show()
#series = Series.fruits_data
year = df1.select('year')
pprint(type(df1.schema["fruits_and_vegetables"].dataType))

df = spark.createDataFrame([(17.00, "2018-03-10T15:27:18+00:00"), # The first six days are sequential
  (13.00, "2018-03-11T12:27:18+00:00"),  # included ...  
  (25.00, "2018-03-12T11:27:18+00:00"),  # included ...
  (20.00, "2018-03-13T15:27:18+00:00"),  # included ...
  (56.00, "2018-03-14T12:27:18+00:00"),  # included...
  (99.00, "2018-03-15T11:27:18+00:00"),  # This one will be included with the next window
  (156.00, "2018-03-22T11:27:18+00:00"), # This one is inside the 7 day window of the previous
  (122.00, "2018-03-31T11:27:18+00:00"), # This one is a new window, outside the 7 day window of any previous...
  (7000.00, "2018-04-15T11:27:18+00:00"),# This starts a * brand new window * with the next entry included next
  (9999.00, "2018-04-16T11:27:18+00:00") # This should be part of the previous entry
  ],
  ["dollars", "timestampGMT"])

df.show()
df = df.withColumn('timestampGMT', df.timestampGMT.cast('timestamp'))

df.show()


windowSpec = Window.orderBy(func.col("timestampGMT").cast('long')).rangeBetween(-days(7), 0)

df2 = df.withColumn('rolling_seven_day_average', func.avg("dollars").over(windowSpec)) 


df2.show()
df2.select('dollars').show()
dat = df2.select('dollars')
date = df2.select('timestampGMT')
dat.show()
date.show()
#gre_histogram = df2.select('dollars').rdd.flatMap(lambda x: x).histogram(10)
#pd.DataFrame(
#    list(zip(*gre_histogram)), 
#    columns=['dollars', 'temp']
#).set_index(
#    'dollars'
#).plot(kind='bar');

#ts = pd.Series(np.random.randn(5), index=pd.date_range('1/1/2000', periods=5))

#ts = ts.cumsum()

#ts.plot()
#fruits_data.collect().show()
#plt.show()
df3 = pd.DataFrame(np.array(fruits_data.collect()), index=year.collect(), columns=list('D'))
df3
def f(x):
    df3.plot()
    plt.show()
#df3 = df3.cumsum()
#df3.plot(kind='line',x=df3.index,y=df3.columns,color='red')
#fig = plt.figure(figsize=(100,100))
#ax = fig.add_subplot(111)
#ax.axvline(df3.index, y=df3.columns, marker='o')
#plt.show()

#df2.timestampGMT
#num_bins = 50
#n, bins, patches = plt.hist(df2.dollars, num_bins, normed=1, facecolor='green', alpha=0.5)

raw_data.foreach(f)

ssc.start()             # Start the computation
ssc.awaitTermination() 