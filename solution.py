from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, countDistinct

spark = SparkSession.builder \
    .appName("Solutions") \
    .getOrCreate()

url = "jdbc:postgresql://localhost:5432/pagila"
props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Загрузка DataFrames
df_category = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "public.category") \
    .options(**{"user": props["user"], "password": props["password"], "driver": props["driver"]}) \
    .load()

df_film_category = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "public.film_category") \
    .options(**{"user": props["user"], "password": props["password"], "driver": props["driver"]}) \
    .load()

# Выполняем join и группировку:
# 1. Соединяем film_category с category по category_id
# 2. Группируем по имени категории и считаем число уникальных film_id
# 3. Сортируем по убыванию
result_df = df_film_category.join(df_category,
                                  df_film_category["category_id"] == df_category["category_id"],
                                  how="inner") \
    .groupBy(df_category["name"].alias("category_name")) \
    .agg(countDistinct(df_film_category["film_id"]).alias("film_count")) \
    .orderBy(desc("film_count"))

# Выводим результат
result_df.show(truncate=False)
