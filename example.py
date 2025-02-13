import findspark

findspark.init()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("my_app").getOrCreate()
# print("Активные Spark сессии:", spark.sparkContext.uiWebUrl)

# ---Чтение из csv и вывод датафрейма-----------------------------------------------------
PATH = "data/customs_data.csv"
df1 = spark.read.csv(PATH, sep=";", header=True)
df1.show(10)

# ---Создание дф из списка кортежей (не выводит через show()), причем схема выводится, значит датафрейм создается-----------------------------------------------------
df2 = spark.createDataFrame(
    [(0, [1, 2, 5]), (1, [1, 2, 3, 5]), (2, [1, 2])], ["id", "items"]
)

# df2.printSchema()
# df2.show()

# ---Создание дф из списка кортежей (не выводит через show()), причем схема выводится, значит датафрейм создаетс-----------------------------------------------------
data_tuples = [(1, "abc"), (2, "def")]
schema = "id int, value string"
df3 = spark.createDataFrame(data_tuples, schema)

# df3.printSchema()
# df3.show()
