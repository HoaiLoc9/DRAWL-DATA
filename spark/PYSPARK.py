from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, trim, col, udf
from pyspark.sql.types import StringType
import re

# Đường dẫn đến tệp JAR driver PostgreSQL
driver_path = "/Users/letranhoailoc/Downloads/postgresql-42.7.4.jar"

# Khởi tạo SparkSession với MongoDB Spark Connector
spark = SparkSession.builder \
    .appName("Game Data Processing") \
    .config("spark.jars", driver_path) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .config("spark.driver.extraClassPath", driver_path) \
    .config("spark.executor.extraClassPath", driver_path) \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/DATASTEAM.games") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/DATASTEAM.games") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .getOrCreate()

# Đọc dữ liệu từ MongoDB
df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017/DATASTEAM.games") \
    .load()

# Xử lý cột _id (chuyển ObjectId sang chuỗi)
df = df.withColumn("_id", col("_id.oid").cast("string"))

# Làm sạch các giá trị như "or better", "or over", "or", "()", "," và "/"
df = df.withColumn('cleaned_cpu', regexp_replace(col('minimum_cpu'), r'\(.*?\)|or better|or over|or|,|\/|\\', ''))

# Loại bỏ khoảng trắng thừa
df = df.withColumn('cleaned_cpu', trim(col('cleaned_cpu')))

# Định nghĩa UDF để chuẩn hóa tên CPU (Intel Core, AMD FX, AMD Ryzen)
def normalize_cpu(cpu_string):
    cpu_string = cpu_string.lower()
    cpu_string = re.sub(r'intel\s*core\s*i\s*([3579])', r'Intel Core i\1', cpu_string, flags=re.IGNORECASE)
    cpu_string = re.sub(r'amd\s*fx\s*-?\s*(\d+)', r'AMD FX-\1', cpu_string, flags=re.IGNORECASE)
    cpu_string = re.sub(r'amd\s*ryzen\s*(\d+)', r'AMD Ryzen \1', cpu_string, flags=re.IGNORECASE)
    return cpu_string

# Tạo UDF với Spark
normalize_cpu_udf = udf(normalize_cpu, StringType())

# Áp dụng UDF vào cột 'cleaned_cpu'
df = df.withColumn('cleaned_cpu', normalize_cpu_udf(col('cleaned_cpu')))

# Tách bảng thành 2 bảng mới: game_companies và games

# Bảng games
df_games = df.select(
    "_id", "name", "description", "game_tag", "release_date"
)

# Bảng game_companies chứa thông tin developer và publisher
df_game_companies = df.select(
    "_id", "developer", "publisher"
).withColumnRenamed("_id", "game_id")

# Bảng game_details
df_game_details = df.select(
    "_id", "final_price_number_discount", "final_price_number_original", "review_summary"
).withColumnRenamed("_id", "game_id")

# Bảng system_requirements
df_system_requirements = df.select(
    "_id", "cleaned_cpu", "minimum_ram", "minimum_rom"
).withColumnRenamed("_id", "game_id") \
 .withColumnRenamed("cleaned_cpu", "minimum_cpu")

# Lưu dữ liệu vào PostgreSQL
df_games.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/datasteam") \
    .option("dbtable", "games") \
    .option("user", "letranhoailoc") \
    .option("password", "8989") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

df_game_companies.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/datasteam") \
    .option("dbtable", "game_companies") \
    .option("user", "letranhoailoc") \
    .option("password", "8989") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

df_game_details.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/datasteam") \
    .option("dbtable", "game_details") \
    .option("user", "letranhoailoc") \
    .option("password", "8989") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

df_system_requirements.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/datasteam") \
    .option("dbtable", "system_requirements") \
    .option("user", "letranhoailoc") \
    .option("password", "8989") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Dữ liệu đã được xử lý và lưu thành công vào PostgreSQL.")
