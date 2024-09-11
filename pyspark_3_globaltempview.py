from pyspark.sql import SparkSession

# Hive 지원을 포함한 Spark 세션 초기화
spark = (SparkSession.builder
         .appName("Pyspark Hive Example")
         .config("spark.sql.warehouse.dir", "hdfs://sc-datalake03.ifl.co.kr:2181/user/hive/warehouse")
         .config("hive.metastore.uris", "thrift://sc-datalake03.ifl.co.kr:9083")
         .enableHiveSupport()
         .getOrCreate())

df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])

df.createGlobalTempView("global_temp_view")

# global Temp View 조회
spark.sql("SELECT * FROM global_temp.global_temp_view").show()
spark.stop()