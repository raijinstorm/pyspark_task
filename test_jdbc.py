from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("TestJDBC") \
    .getOrCreate()
print("JARs:", spark.sparkContext.getConf().get("spark.jars"))
try:
    df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.category") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    print("Columns:", df.columns)
except Exception as e:
    print("Error:", e)
