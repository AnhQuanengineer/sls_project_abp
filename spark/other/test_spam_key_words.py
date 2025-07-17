from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, expr

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("StringToArray").getOrCreate()

# Tạo DataFrame mẫu
data = [("[{match_phrase={content=CẦN GIÚP ĐỠ KHẨN CẤP}}, {match_phrase={content=GIÚP ĐỠ KINH PHÍ MAI TÁNG}}, {match_phrase={content=ÁO QUAN MAI TÁNG}}, {match_phrase={content=HOÀN CẢNH ĐÁNG THƯƠNG}}, {match_phrase={content=quý ân nhân}}, {match_phrase={content=gái gọi}}]",)]
df = spark.createDataFrame(data, ["column_name"])

# Xử lý chuỗi để chuyển thành mảng
df = df.withColumn(
    "column_name_array",
    split(
        # Loại bỏ [ và ], thay }}, { bằng dấu phân tách tạm thời (ví dụ: |)
        regexp_replace(
            col("column_name"),
            r"^\[|\]$|\}, \{match_phrase=\{content=|\}\}",
            "|"
        ),
        r"\|"
    )
)

# Làm sạch từng phần tử trong mảng (loại bỏ khoảng trắng dư thừa)
df = df.withColumn(
    "column_name_array",
    expr("transform(column_name_array, x -> trim(x))")
)

# Loại bỏ phần tử rỗng (nếu có) trong mảng
df = df.withColumn(
    "column_name_array",
    expr("filter(column_name_array, x -> x != '')")
)

# Hiển thị kết quả
df.show(truncate=False)
df.printSchema()