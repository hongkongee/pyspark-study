from pyspark.sql import SparkSession

# Hive 지원을 포함한 Spark 세션 초기화
spark = (SparkSession.builder
         .appName("Pyspark Hive Example")
         .config("spark.sql.warehouse.dir", "hdfs://sc-datalake03.ifl.co.kr:2181/user/hive/warehouse")
         .config("hive.metastore.uris", "thrift://sc-datalake03.ifl.co.kr:9083")
         .enableHiveSupport()
         .getOrCreate())

# Hive 테이블을 조회하는 SQL 쿼리 실행
# df1 = spark.sql("SELECT * FROM tb_risk_prob WHERE risk_prob > 5")
# df1.show()

# Hive 테이블을 DataFrame으로 로드
hive_df = spark.table("tb_risk_prob")

# DataFrame API를 사용하여 데이터 필터링
filtered_df = hive_df.filter(hive_df["risk_prob"] > 5)

# 필터링된 데이터 보기
filtered_df.show()