from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

# Створення сесії Spark
spark = SparkSession.builder.appName("PySpark Data Analysis").getOrCreate()

# ANSI escape codes для кольорів
MAGENTA = "\033[35m"
CYAN = "\033[36m"
YELLOW = "\033[33m"

def load_dataset(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)

try:
    # 1. Завантаження датасетів
    users_df = load_dataset(spark, "data/users.csv")
    purchases_df = load_dataset(spark, "data/purchases.csv")
    products_df = load_dataset(spark, "data/products.csv")
except Exception as e:
    print(f"Помилка під час завантаження датасетів: {e}")

# Виведення кількості рядків до очищення
print(f"{MAGENTA}Кількість рядків у кожному DataFrame до очищення:{CYAN}")
print(f"users: {users_df.count()}")
print(f"purchases: {purchases_df.count()}")
print(f"products: {products_df.count()}")
print(" ")

# 2. Очищення даних, видаляючи будь-які рядки з пропущеними значеннями
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# Виведення кількості рядків після очищення
print(f"{MAGENTA}\nКількість рядків у кожному DataFrame після очищення:{CYAN}")
print(f"users: {users_df.count()}")
print(f"purchases: {purchases_df.count()}")
print(f"products: {products_df.count()}")
print(" ")

# 3. Загальна сума покупок за категорією продуктів
purchases_with_products = purchases_df.join(products_df, "product_id", "inner")
total_sales_by_category = purchases_with_products.groupBy("category").agg(
    round(sum(col("price") * col("quantity")), 2).alias("total_sales")
)

print(f"{MAGENTA}\nЗагальна сума покупок за категорією продуктів:{CYAN}")
total_sales_by_category.show()

# 4. Сума покупок за категорією для вікової категорії від 18 до 25 років
purchases_with_users = purchases_with_products.join(users_df, "user_id", "inner")
sales_18_25 = purchases_with_users.filter((col("age") >= 18) & (col("age") <= 25))

sales_by_category_18_25 = sales_18_25.groupBy("category").agg(
    round(sum(col("price") * col("quantity")), 2).alias("total_sales")
)

print(
    f"{MAGENTA}\nСума покупок за категорією для вікової категорії від 18 до 25 років:{CYAN}"
)
sales_by_category_18_25.show()

# 5. Визначення частки покупок за кожною категорією для вікової категорії 18-25 років
total_sales_18_25 = sales_by_category_18_25.agg(
    sum("total_sales").alias("total")
).collect()[0]["total"]

percentage_sales_by_category_18_25 = sales_by_category_18_25.withColumn(
    "percentage", round((col("total_sales") / total_sales_18_25) * 100, 2)
)

print(
    f"{MAGENTA}\nЧастка покупок за кожною категорією для вікової категорії 18-25 років:{CYAN}"
)
percentage_sales_by_category_18_25.show()

# 6. Вибір 3 категорій з найвищим відсотком витрат
top_3_categories = percentage_sales_by_category_18_25.orderBy(
    col("percentage").desc()
).limit(3)

print(
    f"{MAGENTA}\nТоп 3 категорії продуктів з найвищим відсотком витрат для вікової категорії 18-25 років:{CYAN}"
)
top_3_categories.show()

# Закриття SparkSession
spark.stop()
