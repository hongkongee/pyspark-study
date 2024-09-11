from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder.appName("TempViewExample").getOrCreate()

# 데이터프레임 생성
data = [("Alice", 1), ("Bob", 2)]
columns = ["Name", "ID"]
df = spark.createDataFrame(data, schema=columns)

# Temp View 생성
df.createOrReplaceTempView("people_temp_view")

# Temp View 조회
spark.sql("SELECT * FROM people_temp_view").show()

# SparkSession 종료
spark.stop()

# 새로운 SparkSession 생성
spark_new = SparkSession.builder.appName("NewSession").getOrCreate()

# 같은 뷰 이름으로 쿼리 시도 - 실패할 것임
try:
    spark_new.sql("SELECT * FROM people_temp_view").show()
except Exception as e:
    print("Error:", e)

# 두 번째 SparkSession 종료
spark_new.stop()