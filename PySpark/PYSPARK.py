from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, trim, col, udf, when
from pyspark.sql.functions import to_date, date_format, regexp_extract
from pyspark.sql.functions import monotonically_increasing_id, concat, lit, substring
from pyspark.sql.types import FloatType, StringType
import sys
import io
import os
import logging
import psycopg2  # Thêm thư viện này để kết nối với PostgreSQL và thực thi SQL trực tiếp

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

# Đọc dữ liệu từ MongoDB
df = spark.read.format("mongo").option("uri", "mongodb://mymongodb:27017/DATASTEAM.games").load()

# Xóa các ký tự đặc biệt khỏi cột 'name', 'publisher', và 'developer'
df = df.withColumn("name", regexp_replace(col("name"), r'[^a-zA-Z0-9\s]', ""))
df = df.withColumn("publisher", regexp_replace(col("publisher"), r'[^a-zA-Z0-9\s]', ""))
df = df.withColumn("developer", regexp_replace(col("developer"), r'[^a-zA-Z0-9\s]', ""))
df = df.withColumn("description", regexp_replace(col("description"), r'[^a-zA-Z0-9\s]', ""))
# Lọc bỏ các dòng mà cột 'developer' có giá trị null hoặc chỉ chứa khoảng trắng
df = df.filter((col("developer").isNotNull()) & (trim(col("developer")) != ""))
# Tạo ID bằng cách kết hợp chỉ mục và hai chữ cái đầu tiên của tên game
df = df.withColumn("game_id", concat(substring(col("name"), 1, 2), monotonically_increasing_id().cast(StringType())))

# Chỉ lấy 10 ký tự cuối của ID
df = df.withColumn("game_id", substring(col("game_id"), -10, 10))

# Tạo ID cho publisher
df = df.withColumn("publisher_id", concat(substring(col("publisher"), 1, 2), monotonically_increasing_id().cast(StringType())))
df = df.withColumn("publisher_id", substring(col("publisher_id"), -10, 10))

# Làm sạch các giá trị không cần thiết khỏi cột CPU
df = df.withColumn('cleaned_cpu', when(col('minimum_cpu').isNull(), "").otherwise(col('minimum_cpu')))
df = df.withColumn('cleaned_cpu', regexp_replace(col('cleaned_cpu'), r'\(.*?\)|or better|or over|or|,|\/|\\', ''))
df = df.withColumn('cleaned_cpu', trim(col('cleaned_cpu')))

# Định nghĩa UDF để chuẩn hóa tên CPU
def normalize_cpu(cpu_string):
    if cpu_string is None:
        return "Intel Core i5"

    cpu_string = cpu_string.lower()
    
    for cpu_type in ['i3', 'i5', 'i7', 'i9']:
        if cpu_type in cpu_string:
            return f'Intel Core {cpu_type}'

    return "Intel Core i5"

normalize_cpu_udf = udf(normalize_cpu, StringType())
df = df.withColumn('cleaned_cpu', normalize_cpu_udf(col('cleaned_cpu')))
#xử lý cột review_summary
def review(review_string):
    if review_string == 'Mix':
        return "Mixed"
    if review_string == '3 user reviews':
        return "Mixed" 
    if review_string == '8 user reviews':
        return "Mixed"
    if review_string == '7 user reviews':
        return "Mixed"
    if review_string == '4 user reviews':
        return "Mixed"
    if review_string == '2 user reviews':
        return "Mixed"
    if review_string == '9 user reviews':
        return "Mixed"
    if review_string == '6 user reviews':
        return "Mixed"
    if review_string == '1 user reviews':
        return "Mixed"
    if review_string == '5 user reviews':
        return "Mixed"
    return review_string
review_udf = udf(review, StringType())
df = df.withColumn('review_summary',review_udf(col('review_summary')))
# Xử lý cột release_date
df = df.withColumn("release_date", to_date(col("release_date"), "d MMM, yyyy"))
df = df.withColumn("release_date", date_format(col("release_date"), "dd/MM/yyyy"))

# Làm sạch và chỉ lấy giá trị số cho minimum_ram
df = df.withColumn("minimum_ram_GB", regexp_extract(trim(col("minimum_ram")), r"(\d+)", 1).cast(FloatType()))
# Làm sạch và chỉ lấy giá trị số cho minimum_rom
df = df.withColumn("minimum_rom_GB", regexp_extract(trim(col("minimum_rom")), r"(\d+)", 1).cast(FloatType()))

# Bỏ cột cũ nếu không cần thiết
df = df.drop("minimum_ram", "minimum_rom")

# Xóa bỏ các dòng không thỏa mãn điều kiện
df = df.filter((col("minimum_rom_GB").isNotNull()) & (col("minimum_rom_GB") <= 500)) \
       .filter((col("minimum_ram_GB").isNotNull()) & (col("minimum_ram_GB") <= 16))
# Tách bảng thành các bảng mới
df_games = df.select("game_id", "name", "game_tag", "release_date", "description")
df_game_companies = df.select("game_id", "publisher_id", "developer", "publisher")
df_game_details = df.select("game_id", "final_price_number_discount", "final_price_number_original", "review_summary")
df_system_requirements = df.select("game_id", "cleaned_cpu", "minimum_ram_GB", "minimum_rom_GB")



'''
def save_to_postgres(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/datasteam") \
        .option("dbtable", table_name) \
        .option("user", "tutai") \
        .option("password", "0000") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
'''
# Lưu dữ liệu vào PostgreSQL
def save_to_postgres(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/datasteam") \
        .option("dbtable", table_name) \
        .option("user", "tutai") \
        .option("password", "0000") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


# Ghi dữ liệu vào PostgreSQL
save_to_postgres(df_games, "games")
save_to_postgres(df_game_companies, "game_companies")
save_to_postgres(df_game_details, "game_details")
save_to_postgres(df_system_requirements, "system_requirements")

from pyspark.sql import Row
# Hàm để thực thi các lệnh SQL tạo khóa chính và khóa ngoại
def execute_sql_commands(commands):
    try:
        # Kết nối đến PostgreSQL
        connection = psycopg2.connect(
            host="host.docker.internal",  # Thay localhost bằng host.docker.internal
            port="5432",
            database="datasteam",
            user="tutai",
            password="0000"
        )
        connection.autocommit = True
        cursor = connection.cursor()
        
        # Thực thi từng lệnh SQL
        for command in commands:
            cursor.execute(command)
            logging.info(f"Executed command: {command}")
        
        cursor.close()
        connection.close()
        logging.info("Primary and foreign key constraints created successfully.")
    except Exception as e:
        logging.exception(f"Error executing SQL commands: {e}")

# Danh sách các lệnh SQL để tạo khóa chính và khóa ngoại
commands = [
    # Khóa chính cho bảng games
    "ALTER TABLE games ADD CONSTRAINT pk_games PRIMARY KEY (game_id);",
    "ALTER TABLE game_companies ADD CONSTRAINT pk_companies PRIMARY KEY (publisher_id);",
    # Khóa ngoại trỏ đến game_id trong bảng games
    "ALTER TABLE game_companies ADD CONSTRAINT fk_game_companies_game_id FOREIGN KEY (game_id) REFERENCES games (game_id);",
    "ALTER TABLE game_details ADD CONSTRAINT fk_game_details_game_id FOREIGN KEY (game_id) REFERENCES games (game_id);",
    "ALTER TABLE system_requirements ADD CONSTRAINT fk_system_requirements_game_id FOREIGN KEY (game_id) REFERENCES games (game_id);"
]
# Thực thi các lệnh SQL để tạo khóa chính và khóa ngoại
execute_sql_commands(commands)

print("Dữ liệu đã được xử lý, lưu thành công vào PostgreSQL và ràng buộc đã được tạo.")
