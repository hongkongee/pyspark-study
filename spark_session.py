from pyspark.sql import SparkSession

# SparkSession 생성
spark = (SparkSession.builder
         .appName("Pyspark Hive Example")
         .config("spark.sql.warehouse.dir", "hdfs://sc-datalake03.ifl.co.kr:2181/user/hive/warehouse")
         .config("hive.metastore.uris", "thrift://sc-datalake03.ifl.co.kr:9083")
#         .config("hive.metastore.uris", "thrift://10.0.2.15:9083")
#         .config("spark.yarn.principal", "user@REALM")
#         .config("spark.yarn.keytab", "/path/to/user.keytab")
#         .config("spark.kerberos.access.hadoopFileSystems", "hdfs://your-hdfs-namenode")
         .enableHiveSupport()
         .getOrCreate())

# Spark SQL 구문 실행 예제
# 예를 들어, Hive 테이블에서 데이터 조회
# result_df = spark.sql("SELECT * FROM your_hive_table")
# result_df.show()

databases = spark.sql("SHOW TABLES")
databases.show()

spark.sql("SELECT * FROM cmv_op").show()

# 사용이 끝난 후, SparkSession 종료
spark.stop()