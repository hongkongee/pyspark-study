import findspark
findspark.init()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Hive 지원을 포함한 Spark 세션 초기화
spark = (SparkSession.builder
         .master("local[*]")
         .appName("Pyspark Hive Example")
         .config("spark.sql.warehouse.dir", "hdfs://sc-datalake03.ifl.co.kr:2181/user/hive/warehouse")
         .config("hive.metastore.uris", "thrift://sc-datalake03.ifl.co.kr:9083")
         .enableHiveSupport()
         .getOrCreate())


'''
df = spark.sql("SELECT * FROM tb_risk_prob WHERE risk_prob > 5")
df.createOrReplaceTempView("my_temp_view")

result = spark.sql("SELECT * FROM my_temp_view")
result.show()

# 데이터를 하이브에 저장
df.write.mode("overwrite").saveAsTable("test_hive_table2")
'''



data = [(1, "Alice", 29), (2, "Bob", 31), (3, "Charlie", 25)]

# DataFrame 생성: createDataFrame(data, schema)
df = spark.createDataFrame(data, ["id", "name", "age"])
df.show()