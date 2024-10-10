from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, trim, col, udf
from pyspark.sql.types import StringType
import re
import os
import logging

# Cấu hình ghi log
logging.basicConfig(level=logging.INFO)

# Đường dẫn đến tệp JAR driver PostgreSQL
driver_path = os.getenv("POSTGRES_DRIVER_PATH", "/path/to/postgresql-42.7.4.jar")
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/DATASTEAM.games")

# Khởi tạo SparkSession
try:
    spark = SparkSession.builder \
        .appName("Game Data Processing") \
        .config("spark.jars", driver_path) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .config("spark.driver.extraClassPath", driver_path) \
        .config("spark.executor.extraClassPath", driver_path) \
        .config("spark.mongodb.input.uri", mongo_uri) \
        .config("spark.mongodb.output.uri", mongo_uri) \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()
    logging.info("Spark session initialized successfully.")
except Exception as e:
    logging.exception(f"Error initializing Spark session: {e}")
    exit(1)


# Đọc dữ liệu từ MongoDB vào DataFrame
df = spark.read \
    .format("mongo") \
    .option("uri", "mongodb://mymongodb:27017/DATASTEAM.games") \
    .load()

# Kiểm tra nếu `df` tồn tại
if df is not None:
    # Xử lý dữ liệu
    df = df.withColumn("_id", col("_id.oid").cast("string"))
    df = df.withColumn('cleaned_cpu', regexp_replace(col('minimum_cpu'), r'\(.*?\)|or better|or over|or|,|\/|\\', ''))
    df = df.withColumn('cleaned_cpu', trim(col('cleaned_cpu')))

    # UDF chuẩn hóa tên CPU
    def normalize_cpu(cpu_string):
        cpu_string = cpu_string.lower()
        cpu_string = re.sub(r'intel\s*core\s*i\s*([3579])', r'Intel Core i\1', cpu_string, flags=re.IGNORECASE)
        cpu_string = re.sub(r'amd\s*fx\s*-?\s*(\d+)', r'AMD FX-\1', cpu_string, flags=re.IGNORECASE)
        cpu_string = re.sub(r'amd\s*ryzen\s*(\d+)', r'AMD Ryzen \1', cpu_string, flags=re.IGNORECASE)
        return cpu_string

    normalize_cpu_udf = udf(normalize_cpu, StringType())
    df = df.withColumn('cleaned_cpu', normalize_cpu_udf(col('cleaned_cpu')))

    # Tách bảng
    df_games = df.select("_id", "name", "description", "game_tag", "release_date")
    df_game_companies = df.select("_id", "developer", "publisher").withColumnRenamed("_id", "game_id")
    df_game_details = df.select("_id", "final_price_number_discount", "final_price_number_original", "review_summary").withColumnRenamed("_id", "game_id")
    df_system_requirements = df.select("_id", "cleaned_cpu", "minimum_ram", "minimum_rom").withColumnRenamed("_id", "game_id").withColumnRenamed("cleaned_cpu", "minimum_cpu")

    # Lưu vào PostgreSQL
    for df_table, table_name in zip([df_games, df_game_companies, df_game_details, df_system_requirements],
                                    ["games", "game_companies", "game_details", "system_requirements"]):
        try:
            df_table.write \
                .format("jdbc") \
                .option("url", os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/datasteam")) \
                .option("dbtable", table_name) \
                .option("user", os.getenv("POSTGRES_USER", "letranhoailoc")) \
                .option("password", os.getenv("POSTGRES_PASSWORD", "8989")) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
            logging.info(f"Data for {table_name} saved successfully.")
        except Exception as e:
            logging.error(f"Error saving data for {table_name}: {e}")
else:
    logging.error("DataFrame `df` is not defined.")
    exit(1)

print("Dữ liệu đã được xử lý và lưu thành công vào PostgreSQL.")
