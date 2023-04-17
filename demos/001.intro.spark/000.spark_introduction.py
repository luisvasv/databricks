# Databricks notebook source
# MAGIC %md
# MAGIC # INTRODUCCIÓN A SPARK
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRvRtWDzqERSJVoMN6_OAaeQpLvmLY5PTwQHT6qHspHgf-9_OVVSBa0dyBmfLuOyjUZb_o&usqp=CAU" alt="ConboBox" style="width: 200; ">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. QUE DEBO TENER EN CUENTA ANTES DE PROGRAMAR? 
# MAGIC 
# MAGIC Aplica para `Spark` y en muchos conceptos aplican para otros lenguajes.

# COMMAND ----------

"""
prerequisitos :
  Estrategia
    1. cuantos datos se van a procesar      : 14846
    2. cuanto pesan los datos               : 1.5M
    3. entender la naturaleza de los datos  :
       3.1 : los datos vienen con o sin header, cuantas columnas y que formato 
    4. como abordar la solución
       4.1 fase I : simular los datos
       4.2 fase II: simulación exitosa, procesar todos los datos
   
   Programación
       5.1 crear la lista de los elementos simulados(muestra con datos del negocio representativa)
       5.2 leer necesidad del problema
       5.3 crear metodos generales
       5.4 resolver problema especifico
       
 IMPORTANTE: nunca validar con cantidades grandes de data
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SPARK CONFIGURAR SPARK SESSION
# MAGIC 
# MAGIC simulación de datos, entendiendo spark
# MAGIC   
# MAGIC   * **cli**        : (*command line interface*) nos entrega el contexto
# MAGIC   * **databricks** : se nos entrega el contexto con el nombre spark
# MAGIC   * **programas**  : nostros debemos crear el spark session, ejemplo:
# MAGIC   
# MAGIC   ```
# MAGIC     # example local environments
# MAGIC 
# MAGIC     from pyspark import SparkConf, SparkContext
# MAGIC     import findspark
# MAGIC     findspark.init()
# MAGIC 
# MAGIC     conf = SparkConf().setAppName(APP_NAME)
# MAGIC     sc = SparkContext(conf=conf)
# MAGIC   ```

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ENTENDIENDO LOS REQUERIMIENTOS
# MAGIC 
# MAGIC la `unal`, quiere hacer una lectura de los tweets que realizán los estudiantes que siguen a la universidad, para conocer cuál ha sido el tema más discutido en sus redes sociales, para esto, contrató a los estudiantes de analitica de grandes datos para realizar este proceso.
# MAGIC 
# MAGIC Ejemplo datos de entrada:
# MAGIC 
# MAGIC ```
# MAGIC "@VirginAmerica amazing to me that we can't get any cold air from the vents. #VX358 #noair #worstflightever #roasted #SFOtoBOS"
# MAGIC "I cannot fly again with airline pepito, #worstflightever"
# MAGIC ```
# MAGIC Datos de salida:
# MAGIC 
# MAGIC | hashtag          | cantidad |
# MAGIC |------------------|----------|
# MAGIC | #VX358           | 1        |
# MAGIC | #noair           | 1        |
# MAGIC | #worstflightever | 2        |
# MAGIC | #roasted         | 1        |
# MAGIC | #SFOtoBOS        | 1        |
# MAGIC 
# MAGIC 
# MAGIC **REQUERIMIENTOS ADICIONALES**
# MAGIC 
# MAGIC Deseo tener una tabla donde me permita tener todos los siguientes indicadores:
# MAGIC                   
# MAGIC   - mensaje
# MAGIC   - longitud del mensaje
# MAGIC   - lista de hashtags
# MAGIC   - longitud de la lista de hasthgs
# MAGIC   - cuantas letras mayusculas uso
# MAGIC   - cuantas letras minusculas uso
# MAGIC   - cuantos numeros uso
# MAGIC   
# MAGIC esto con el fin de que mis usuarios funciones, pueda generar datos y diseñar estrategias a partir de los mensajes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. PROGRAMACIÓN 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 PYTHON
# MAGIC 
# MAGIC Función extractora de información

# COMMAND ----------

# genera una funcion para extrar hashtags
import re

test = "@VirginAmerica amazing to me that we can't get any cold air from the vents. #unal #11 #VX358 #noair #worstflightever #roasted #SFOtoBOS #iloveyou #1212"
def get_hashtags(value):
  return re.findall(r'\B#\w*[a-zA-Z]+\w*', value)

get_hashtags(test)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 SPARK

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.1 HIVE VARIABLES

# COMMAND ----------

spark.sql("SET demo.config.db=unal")
spark.conf.set('demo.config.tbname','twitter')
spark.conf.set('demo.config.tmptable','temp_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "${demo.config.db}" AS bd, 
# MAGIC        "${demo.config.tbname}" AS tbname,
# MAGIC        "${demo.config.tmptable}" AS tmp_table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${demo.config.db};

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.2 RDD & OPERACIONES

# COMMAND ----------

# 1.a partir del archivo de insumo propuesto en el curso se debe cargar los datos y convertir a una lista
# rdd = spark.sparkContext('/ruta/archivo')
rdd = spark.sparkContext.parallelize([
  "@VirginAmerica amazing to me that we can't get any cold air from the vents. #VX358 #noair #worstflightever #roasted #SFOtoBOS",
  "@VirginAmerica LAX to EWR - Middle seat on a red eye. Such a noob maneuver. #sendambien #andchexmix",
  "@VirginAmerica hi! I just bked a cool birthday trip with you, but i can't add my elevate no. cause i entered my middle name during Flight Booking Problems ðŸ˜¢",
  "@VirginAmerica Are the hours of operation for the Club at SFO that are posted online current?",
  "@VirginAmerica help, left expensive headphones on flight 89 IAD to LAX today. Seat 2A. No one answering L&amp",
  "@VirginAmerica awaiting my return phone call, just would prefer to use your online self-service option :(",
  "@VirginAmerica this is great news!  America could start flights to Hawaii by end of year http://t.co/r8p2Zy3fe4 via @Pacificbiznews",
  "Nice RT @VirginAmerica: Vibe with the moodlight from takeoff to #sendambien. #MoodlitMonday #ScienceBehindTheExperience http://t.co/Y7O0uNxTQP",
  "@VirginAmerica Moodlighting is the only way to fly! Best experience EVER! Cool and calming. ðŸ’œâœˆ #MoodlitMonday",
  "@VirginAmerica @freddieawards Done and done! Best airline around, hands down! #sendambien"
])

rdd.take(5)

# COMMAND ----------

# generar lista de twitts que superen una longitud de 100 caracteres
question_2 = rdd.filter(lambda data: len(data) > 100)
print("antes de filter    : ", rdd.count())
print("despues del filter : ", question_2.count())

# COMMAND ----------

print(type(str.upper))

# COMMAND ----------

# se debe crear un proceso que convierta cada twitter en minuscula y mayuscula
question_3_1 = rdd.map(str.upper)
question_3_2 = rdd.map(str.lower)

print("muesta mayusculas   : ",question_3_1.take(1))
print("muestras minusculas : ",question_3_2.take(1))

# COMMAND ----------

def convert_to_uper(value):
  return value.upper()

# convertir a mayusculas camino largo
xx = rdd.map(convert_to_uper)
xx.take(1)

# COMMAND ----------

# convertir a mayusculas camino largo
yasmin = rdd.map(lambda value: value.upper())
yasmin.take(1)

# COMMAND ----------

# se debe obtener un proceso que me permita obtener la longitud de cada uno de los twitts
question_4 = rdd.map(lambda value: (value, len(value)))
question_4.take(2)

# COMMAND ----------

# 5.se debe crear un proceso que me permita solo obtener los hastags(los que empiecen por #)
"""
entrada :
  #unal #analitica #spark
salida:
 #unal
 #nalitica
 #spark
"""
question_5 = rdd.filter(lambda data: "#" in data).flatMap(get_hashtags)
question_5.take(5)

# COMMAND ----------

# se debe crear un proceso que me tenga un acumulador de los hastag mas utilizados
"""
  fase0 : filtrado de datos que tengan #
  fase1 : #unal, #analitica, #unal
  fase2 :
          #unal
          #analitica
          #unal
  fase3 : 
  
          #unal       1
          #analitica  1
          #unal       1
        
  fase4 : 
          #analitica 1
          #unal 2
          
  fase5 :
          
         #unal 2
         #analitica 1
         
  
"""

question_6 = rdd.filter(lambda data: "#" in data)\
                .flatMap(get_hashtags)\
                .map(lambda data: (data, 1))\
                .reduceByKey(lambda value, counter : value + counter)\
                .sortBy(lambda value: value[1], ascending=False)
question_6.take(3)

# COMMAND ----------

rdd.filter(lambda data: "#" in data)\
                .flatMap(get_hashtags)\
                .map(lambda data: (data, 1))\
                .reduceByKey(lambda value, counter : value + counter).take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.3 SOLUCIÓN DE REQUERIMIENTOS

# COMMAND ----------

"""
  requerimiento : deseo tener una tabla donde me permita tener todos los siguientes indicadores:
                  
                  - mensaje
                  - longitud del mensaje
                  - lista de hashtags
                  - longitud de la lista de hasthgs
                  - cuantas letras mayusculas uso
                  - cuantas letras minusculas uso
                  - cuantos numeros uso

                 esto con el fin de que mis usuarios funciones, pueda generar datos y diseñar estrategias a partir de los mensajes
"""

# COMMAND ----------

# solución al requerimiento

def size_hashtags(value):
  size_message = len(value)
  hashtags = re.findall(r'\B#\w*[a-zA-Z]+\w*', value)
  size_hashtags = len(hashtags)
  upper_cases = len([letter for letter in value if letter.isupper()])
  lower_cases = len([letter for letter in value if letter.islower()])
  digit_cases = len([letter for letter in value if letter.isdigit()])
  return value, size_message, upper_cases, lower_cases, digit_cases, hashtags, size_hashtags
  
size_hashtags(test)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.4 TRABAJANDO CON DATAFRAMES
# MAGIC 
# MAGIC Se pueden usar `spark.dataframes` para leer datos, si los datos leen formatos que tienen `schema` y no estan corruptos. De lo contrario, se debe definir para poder tener compatibilidad `match`
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2017/02/blog-illustration-01.png" alt="ConboBox"  width="45%">
# MAGIC </div>

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
  StructField("message", StringType()),
  StructField("message_size", IntegerType()),
  StructField("upper_cases", IntegerType()),
  StructField("lower_cases", IntegerType()),
  StructField("digit_cases", IntegerType()),
  StructField("hashtags", ArrayType(StringType())),
  StructField("hashtags_size", IntegerType())
])

schema


# COMMAND ----------

rdd_advance = rdd.map(size_hashtags)
rdd_advance.take(2)

# COMMAND ----------

df = spark.createDataFrame(rdd_advance, schema)

# COMMAND ----------

# df.show(truncate=False)
# df.show(#)
df.show()

# COMMAND ----------

df.describe()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select(df.message_size).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.5 TRABAJANDO CON SPARK SQL

# COMMAND ----------

# registro de tablas en memoria 
df.createOrReplaceTempView("${demo.config.tmptable}")

# COMMAND ----------

# sirve para consultar datos en memoria o persistidos
spark.sql("""
select hashtags_size, explode(hashtags)
from ${demo.config.tmptable}
""").show()


# COMMAND ----------

# sirve para consultar datos en memoria o persistidos
spark.sql("""
select hashtags_size, explode(hashtags)
from temp_table
""").show()

# COMMAND ----------

df.write.mode("append").saveAsTable("{}.{}".format(
spark.conf.get("demo.config.db"),
spark.conf.get("demo.config.tbname")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 TRABAJANDO SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.3.1 HIVE METASTORE

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE unal;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE ${demo.config.tbname};

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM ${demo.config.tbname};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- formatos recomandados para big data : avro(serializado), parquet(columnar)
# MAGIC SHOW CREATE TABLE twitter;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT message_size, hashtags_size FROM ${demo.config.tbname};

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.3.2 SPARK

# COMMAND ----------

df_mensajes = spark.sql("select message_size, upper_cases, lower_cases, hashtags_size from ${demo.config.tbname}")

# COMMAND ----------

df_mensajes.show()

# COMMAND ----------

df_mensajes.printSchema()
