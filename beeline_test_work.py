import glob
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType


spark = SparkSession.builder.appName("beeline_test_work").getOrCreate()

data_directory = "resources"
path_customer = f"{data_directory}/customer.csv"
path_product = f"{data_directory}/product.csv"
path_order = f"{data_directory}/order.csv"
path_favorite_product = f"{data_directory}/favorite_product"

schema_customer = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("joinDate", DateType(), True),
    StructField("status", StringType(), True)
])

schema_product = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("numberOfProducts", IntegerType(), True)
])

schema_order = StructType([
    StructField("customerID", IntegerType(), True),
    StructField("orderID", IntegerType(), True),
    StructField("productID", IntegerType(), True),
    StructField("numberOfProduct", IntegerType(), True),
    StructField("orderDate", DateType(), True),
    StructField("status", StringType(), True)
])

schema_favorite_product = StructType([
    StructField("customerName", StringType(), True),
    StructField("productName", StringType(), True)
])

customer = spark.read\
    .schema(schema_customer)\
    .option("sep", "\t")\
    .csv(path_customer)\
    .selectExpr("id as customerID", "name as customerName")

product = spark.read\
    .schema(schema_product)\
    .option("sep", "\t")\
    .csv(path_product)\
    .selectExpr("id as productID", "name as productName", "price")

order = spark.read\
    .schema(schema_order)\
    .option("sep", "\t")\
    .csv(path_order)

order_sum = order\
    .filter(f.col("status") == "delivered")\
    .groupBy("customerID", "productID")\
    .agg(f.sum("numberOfProduct").alias("sum_num"))

# выбор любимого продукта по количеству потраченных денег
# order_sum = order\
#     .filter(f.col("status") == "delivered")\
#     .join(product, "productID", "inner")\
#     .withColumn("costOfProduct", f.col("numberOfProduct") * f.col("price"))\
#     .drop("numberOfProduct")\
#     .withColumnRenamed("costOfProduct", "numberOfProduct")\
#     .groupBy("customerID", "productID")\
#     .agg(f.sum("numberOfProduct").alias("sum_num"))

order_sum.show()

favorite_id = order_sum\
    .groupBy("customerID")\
    .agg(f.max("sum_num").alias("sum_num"))\
    .join(order_sum, ["customerID", "sum_num"], "inner")

favorite_product = product\
    .join(favorite_id, "productID", "inner")\
    .join(customer, "customerID", "right")\
    .select("customerID", "customerName", "productName")\
    .orderBy("customerID", "productName")

# favorite_product.show()

favorite_product\
    .repartition(1)\
    .write\
    .mode("overwrite")\
    .option("sep", "\t")\
    .csv(path_favorite_product)

spark.stop()

shutil.move(
    glob.glob(path_favorite_product + "/*.csv")[0],
    path_favorite_product + ".csv"
)
shutil.rmtree(path_favorite_product)


