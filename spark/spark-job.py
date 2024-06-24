from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col
# import pymongo

# Создание сессии Spark с конфигурацией для подключения к MongoDB
spark = SparkSession.builder.appName("Kinopoisk").getOrCreate()

logger = spark._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

# Чтение данных из MongoDB
df = spark.read.format("mongo").load()

# Обработка данных (например, показ первых 5 записей)


# Предобработка данных (Пример: Извлечение некоторых полей и индексирование категорий)
df = df.select("name", "year", df["rating.kp"].alias("rating_kp"), df["votes.kp"].alias("votes_kp"))

df.show(5)
df = df.dropna()

# Индексирование категориальных переменных
indexer = StringIndexer(inputCol="name", outputCol="nameIndex")
df = indexer.fit(df).transform(df)

# Формирование вектора признаков
assembler = VectorAssembler(inputCols=["year", "votes_kp", "nameIndex"], outputCol="features")
df = assembler.transform(df)

# Разделение на тренировочные и тестовые данные
train_data, test_data = df.randomSplit([0.8, 0.2])

# Обучение модели линейной регрессии
lr = LinearRegression(labelCol="rating_kp", featuresCol="features")
lr_model = lr.fit(train_data)

# Предсказания
predictions = lr_model.transform(test_data)
predictions.select("features", "rating_kp", "prediction").show(10)

# Оценка модели
evaluator = RegressionEvaluator(labelCol="rating_kp", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse}")

spark.stop()
