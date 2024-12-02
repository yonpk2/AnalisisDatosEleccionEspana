# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Usaremos **Databricks** porque nos olvidamos de usar una maquina virtual y tenemos todas las funciones SQL, lenguaje python, spark, etc. que nos ayudan a manipular la información
# MAGIC ### 2. Cargar los diferentes ficheros en las herramientas seleccionadas y sacar un listado de sus contenidos por pantalla o a fichero.
# MAGIC
# MAGIC
# MAGIC Cargamos los DataSets primero "municipios" y despues "elecciones" para después unirlas

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/PECMunicipios.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"


# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type)\
    .option("inferSchema", infer_schema)\
    .option("header", first_row_is_header)\
    .option("encoding", "ISO-8859-1")\
    .option("sep", delimiter)\
    .load(file_location)


display(df)

# COMMAND ----------

#acotamos la tabla
from pyspark.sql.functions import trim

for col_name in df.columns:
    df = df.withColumn(col_name, trim(df[col_name]))

display(df)

# COMMAND ----------

#bajamos a lowercase los headers para no tener problemas al manejar los datos
import re
from pyspark.sql import functions as F

def clean_column_names(df):
    new_columns = [re.sub(r'[^a-zA-Z0-9_]', '', col).lower() for col in df.columns]
    return df.toDF(*new_columns)

df = clean_column_names(df)
display(df)

# COMMAND ----------

#Quitamos acentos y "normalizamos la información"
import unicodedata

def remove_accents(input_str):
    if input_str is not None:
        nfkd_form = unicodedata.normalize('NFKD', input_str)
        return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
    return None
  

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

remove_accents_udf = udf(remove_accents, StringType())

for col in df.columns:
    df = df.withColumn(col, remove_accents_udf(F.col(col)))


display(df)

# COMMAND ----------

#como lo muestra la tabla a pesar de haber importado con el tipo de dato definido lo tenemos que volver a definir "codigo" y "poblacion" son enteros
from pyspark.sql.functions import col
columnas_int = ["codigo", "poblacion"]
for columna in columnas_int:
    df = df.withColumn(columna, col(columna).cast("int"))

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "municipios_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "pecmunicipios_csv"

#df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   `municipios_csv`

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/PECElecciones.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("encoding", "ISO-8859-1")\
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

#Limpiamos y ponemos columnas a lowercase
import re
from pyspark.sql import functions as F

def clean_column_names(df):
    new_columns = [re.sub(r'[^a-zA-Z0-9_]', '', col).lower() for col in df.columns]
    return df.toDF(*new_columns)

df = clean_column_names(df)
display(df)

# COMMAND ----------

#vemos la tabla que hicimos primero
df2=spark.table("municipios_csv")
display(df2)

# COMMAND ----------

#Hacemos el join de las tablas con "codigo"
df3=df2.join(df,df2['Codigo']==df['Codigo'],"inner")
df3=df3.drop(df['Codigo'])
display(df3)

# COMMAND ----------

# Create a view or table

temp_table_name = "elecciones_join"

df3.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from elecciones_join

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/elecciones_join", recurse=True)

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "elecciones_join"

df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.Generar un fichero con el top 10 de población de municipios de España y otro con el bottom 10 de población. Necesitamos el nombre de los municipios, autonomía, provincia y población.
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/mnt/tmp/")

# Leer la tabla 'elecciones_join' en un DataFrame
df = spark.sql("SELECT * FROM elecciones_join")

# Seleccionar las columnas necesarias y convertir los valores de población a tipo entero
df = df.selectExpr("Municipio", "Comunidad", "Provincia", "CAST(Poblacion AS INT) as Poblacion")

# Top 10 por población
top_10 = df.orderBy(df.Poblacion.desc()).limit(10)

# Bottom 10 por población
bottom_10 = df.orderBy(df.Poblacion.asc()).limit(10)

# Guardar los resultados como archivos CSV en Databricks File System (DBFS)
top_10.write.csv("/dbfs/tmp/top_10_municipios.csv", header=True, mode="overwrite")
bottom_10.write.csv("/dbfs/tmp/bottom_10_municipios.csv", header=True, mode="overwrite")


# COMMAND ----------

# MAGIC %md
# MAGIC Nos interesan Municipio, Comunidad, Provincia y Poblacion.
# MAGIC Convertimos Poblacion a un tipo numérico para poder ordenar.
# MAGIC Ordenar y filtrar:
# MAGIC
# MAGIC orderBy ordena por población en orden descendente (Top 10) o ascendente (Bottom 10).
# MAGIC limit(10) extrae solo los primeros 10 registros después del ordenamiento.
# MAGIC Guardar los resultados:
# MAGIC
# MAGIC Usamos el método write.csv para exportar los resultados en formato CSV.
# MAGIC La opción header=True asegura que las cabeceras estén incluidas en el archivo.

# COMMAND ----------

display(top_10)

# COMMAND ----------

display(bottom_10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Queremos saber los 10 municipios donde ha habido más participación (el porcentaje de votos respecto el censo es más alto) y donde ha habido más abstención. Generaremos un fichero del top y uno del bottom con los datos del municipio y el % de participación.
# MAGIC

# COMMAND ----------

# Leer la tabla 'elecciones_join'
df = spark.sql("SELECT * FROM elecciones_join")

# Seleccionar columnas relevantes y calcular participación y abstención
df = df.selectExpr(
    "Municipio",
    "Comunidad",
    "Provincia",
    "CAST(Votantes AS FLOAT) / CAST(Censo AS FLOAT) * 100 as Participacion",
    "100 - (CAST(Votantes AS FLOAT) / CAST(Censo AS FLOAT) * 100) as Abstencion"
).filter("Censo > 0")  # Filtrar para evitar divisiones por cero

# Top 10 municipios con mayor participación
top_participacion = df.orderBy("Participacion", ascending=False).limit(10)

# Bottom 10 municipios con mayor abstención
bottom_abstencion = df.orderBy("Abstencion", ascending=False).limit(10)

# Guardar los resultados como archivos CSV en DBFS
top_participacion.write.csv("dbfs:/mnt/tmp/top_participacion.csv", header=True, mode="overwrite")
bottom_abstencion.write.csv("dbfs:/mnt/tmp/bottom_abstencion.csv", header=True, mode="overwrite")


# COMMAND ----------

# MAGIC %md
# MAGIC Cálculo de participación y abstención:
# MAGIC
# MAGIC CAST asegura que las divisiones se realicen correctamente como números flotantes.
# MAGIC La fórmula para participación y abstención está incluida en el selectExpr.
# MAGIC Ordenamiento:
# MAGIC
# MAGIC orderBy("Participacion", ascending=False): Ordena en orden descendente para el Top 10.
# MAGIC orderBy("Abstencion", ascending=False): Ordena en orden descendente para el Bottom 10.
# MAGIC Guardar resultados:
# MAGIC
# MAGIC Los archivos se guardan en la ruta dbfs:/mnt/tmp/ para accesibilidad.
# MAGIC Incluyen encabezados gracias a header=True.

# COMMAND ----------

# Leer y mostrar el Top 10 de participación
top_part_df = spark.read.csv("dbfs:/mnt/tmp/top_participacion.csv", header=True, inferSchema=True)
display(top_part_df)



# COMMAND ----------

# Leer y mostrar el Bottom 10 de abstención
bottom_abs_df = spark.read.csv("dbfs:/mnt/tmp/bottom_abstencion.csv", header=True, inferSchema=True)
display(bottom_abs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.¿Existe algún municipio que haya votado al 100% a un partido? Si es así, ¿cuál es y a qué partido?. Si no es así, sacar una lista de los 10 municipios donde su concentración de voto porcentual haya sido mayor y a qué partido.

# COMMAND ----------

df = spark.sql("SELECT * FROM elecciones_join")
print(df.columns)


# COMMAND ----------

from pyspark.sql.functions import col, greatest, expr

# Leer la tabla 'elecciones_join'
df = spark.sql("SELECT * FROM elecciones_join")

# Lista de columnas que corresponden a los partidos
partidos = [
    'pp', 'psoe', 'podemosiuequo', 'cs', 'ecp', 'podemoscompromseupv', 'erccats', 'cdc', 'podemosenmareaanovaeu', 'eajpnv', 'ehbildu', 'ccapnc', 'pacma', 'recortescerogrupoverde', 'upyd', 'vox', 'bngns', 'pcpe', 'gbai', 'eb', 'fedelasjons', 'si', 'somval', 'ccd', 'sain', 'ph', 'centromoderado', 'plib', 'ccdci', 'upl', 'pcoe', 'and', 'jxc', 'pfyv', 'cilus', 'pxc', 'mas', 'izar', 'unidaddelpueblo', 'prepal', 'ln', 'repo', 'independientesfia', 'entaban', 'imc', 'puede', 'fe', 'alcd', 'fme', 'hrtsln', 'udt'
]

# Convertir votos válidos y los votos de los partidos a flotantes para el cálculo
df = df.select(
    "Municipio", "Comunidad", "Provincia", 
    col("Validos").cast("float").alias("VotosValidos"), 
    *[col(f"`{partido}`").cast("float").alias(partido) for partido in partidos]
).filter("VotosValidos > 0")  # Filtrar para evitar divisiones por cero

# Calcular el porcentaje de votos por partido
for partido in partidos:
    df = df.withColumn(f"{partido}_Porcentaje", (col(partido) / col("VotosValidos")) * 100)

# Calcular el porcentaje máximo y el partido asociado
df = df.withColumn(
    "Max_Porcentaje",
    greatest(*[col(f"{partido}_Porcentaje") for partido in partidos])
)

# Construcción explícita del expr para evitar f-strings
array_expr = "array(" + ", ".join([f'"{partido}"' for partido in partidos]) + ")"
position_expr = "array(" + ", ".join([f"`{partido}_Porcentaje`" for partido in partidos]) + ")"

df = df.withColumn(
    "Partido_Maximo",
    expr(f"{array_expr}[array_position({position_expr}, Max_Porcentaje)]")
)

# Filtrar municipios con el 100% de votos para un partido
municipios_100 = df.filter("Max_Porcentaje = 100")

# Verificar si existen municipios con 100% de votos
if municipios_100.count() > 0:
    print("Municipios con 100% de votos a un partido:")
    municipios_100.select("Municipio", "Partido_Maximo", "Max_Porcentaje").show()
    
    # Guardar resultados
    municipios_100.write.csv("dbfs:/mnt/tmp/municipios_100.csv", header=True, mode="overwrite")
else:
    # Obtener el Top 10 de municipios con mayor concentración de votos
    top_10_municipios = df.orderBy("Max_Porcentaje", ascending=False).limit(10)
    print("Top 10 municipios con mayor concentración de votos:")
    top_10_municipios.select("Municipio", "Partido_Maximo", "Max_Porcentaje").show()
    
    # Guardar resultados
    top_10_municipios.write.csv("dbfs:/mnt/tmp/top_10_concentracion.csv", header=True, mode="overwrite")


# COMMAND ----------

# MAGIC %md
# MAGIC Como podemos ver Castilnuevo, Congostrina, Rebollosa, La vid de Bureba, Portillo de Soria y Valdemadera fueron los municipios con 100% de votos para el partido PSOE

# COMMAND ----------

# Leer municipios con 100% de votos
municipios_100_df = spark.read.csv("dbfs:/mnt/tmp/municipios_100.csv", header=True, inferSchema=True)
display(municipios_100)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.Necesitamos comparar los datos de participación de la ‘España vacía’ con los de la ‘España llena’. Saquemos el índice de participación (votos/censo) por provincia, ordenado de mayor a menor.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sum, expr

# Leer la tabla 'elecciones_join'
df = spark.sql("SELECT * FROM elecciones_join")

# Convertir censo y votantes a valores numéricos para cálculos
df = df.select(
    "Provincia",
    col("Censo").cast("float").alias("censo"),
    col("Votantes").cast("float").alias("votantes")
)

# Calcular índice de participación (votos/censo) por provincia
df_participacion = df.groupBy("Provincia").agg(
    sum("votantes").alias("total_votantes"),
    sum("censo").alias("total_censo")
).withColumn(
    "indice_participacion",
    (col("total_votantes") / col("total_censo")) * 100
)

# Ordenar las provincias por índice de participación de mayor a menor
df_participacion_ordenado = df_participacion.orderBy(col("indice_participacion").desc())

# Mostrar resultados
print("Índice de participación por provincia:")
df_participacion_ordenado.select("Provincia", "indice_participacion").show(truncate=False)

# Guardar los resultados en un fichero CSV
df_participacion_ordenado.write.csv("dbfs:/mnt/tmp/indice_participacion_provincia.csv", header=True, mode="overwrite")


# COMMAND ----------

#filtro para 'España vacía' (provincias de baja densidad)
provincias_vacia = ["Soria", "Teruel", "Cuenca", "Zamora", "Palencia", "Ávila", "Segovia"]
provincias_llena = ["Madrid", "Barcelona", "Valencia", "Sevilla", "Málaga"]

# Filtrar las provincias de la 'España vacía'
df_vacia = df_participacion_ordenado.filter(col("Provincia").isin(provincias_vacia))
df_llena = df_participacion_ordenado.filter(col("Provincia").isin(provincias_llena))

# Mostrar resultados
print("Participación en España vacía:")
df_vacia.show(truncate=False)

print("Participación en España llena:")
df_llena.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Queremos saber si los municipios grandes son representativos en los resultados de las comunidades. Debemos sacar para cada comunidad el municipio que tiene más población y comparar el partido con más participación del municipio con la comunidad. ¿Coinciden? ¿No coinciden? ¿Tiene que ver con que represente más de un determinado porcentaje de población de la comunidad?
# MAGIC

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import col, sum, expr

# Leer la tabla 'elecciones_join'
df = spark.sql("SELECT * FROM elecciones_join")
# Definimos una ventana para ordenar por comunidad y población
windowSpec = Window.partitionBy("comunidad").orderBy(col("poblacion").cast("int").desc())

# Filtrar para obtener el municipio más grande de cada comunidad
municipios_grandes = df.withColumn("row", row_number().over(windowSpec)) \
                       .filter(col("row") == 1) \
                       .drop("row")


# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# Definimos una ventana para ordenar por comunidad y población
windowSpec = Window.partitionBy("comunidad").orderBy(col("poblacion").cast("int").desc())

# Agregamos una columna para numerar las filas y filtramos el municipio más grande de cada comunidad
municipios_representativos = df.withColumn("row", row_number().over(windowSpec)) \
.filter(col("row") == 1) \
.drop("row")

display(municipios_representativos)


# COMMAND ----------

#Primero, seleccionaremos el municipio con la mayor población dentro de cada comunidad.
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# Definimos una ventana para ordenar por comunidad y población
windowSpec = Window.partitionBy("Comunidad").orderBy(col("Poblacion").cast("int").desc())

# Agregaremos una columna que numere las filas y filtrar para obtener el municipio más grande de cada comunidad
municipios_representativos = df.withColumn("row", row_number().over(windowSpec)) \
.filter(col("row") == 1) \
.drop("row")

display(municipios_representativos)

# COMMAND ----------

from pyspark.sql.functions import expr, col
from pyspark.sql.window import Window

# Seleccionamos las columnas de interés y transformamos las columnas de partidos en filas
partidos_municipios_representativos = municipios_representativos.select("Comunidad", "Municipio", "Poblacion",
    expr("stack(5, 'pp', `pp`, 'psoe', `psoe`, 'podemos-iu-equo', `podemosiuequo`, 'cs', `cs`, 'erc-cats', `erccats`) as (Partido, Votos)")
)

# Creamos una ventana para ordenar por votos y seleccionar el partido con más votos en cada municipio
windowPartido = Window.partitionBy("Comunidad", "Municipio").orderBy(col("Votos").cast("int").desc())
partido_ganador_municipio = partidos_municipios_representativos.withColumn("row", row_number().over(windowPartido)) \
.filter(col("row") == 1) \
.drop("row")

display(partido_ganador_municipio)


# COMMAND ----------

from pyspark.sql.functions import expr

# Convertimos los votos de partidos en filas para facilitar el agrupamiento por comunidad
partidos_comunidad = df.select("comunidad",
    expr("stack(5, 'pp', `pp`, 'psoe', `psoe`, 'podemos-iu-equo', `podemosiuequo`, 'cs', `cs`, 'erc-cats', `erccats`) as (partido, votos)")
)

display(partidos_comunidad)


# COMMAND ----------

# Calculamos la población total de cada comunidad
poblacion_comunidad = df.groupBy("comunidad").agg({"poblacion": "sum"}) \
                        .withColumnRenamed("sum(poblacion)", "poblacion_total")

# Unimos con un join la población total con el DataFrame `partido_ganador_municipio`
partido_ganador_municipio = partido_ganador_municipio.join(poblacion_comunidad, "comunidad")

# Evitamos que tenga columnas repetidas entre ambos datasets antes de realizar el join
# Eliminamos la columna `poblacion_total` en `partido_ganador_municipio` si ya existe
partido_ganador_municipio = partido_ganador_municipio.drop("poblacion_total")

# Realizamos el join de nuevo con la población total y seleccionamos las columnas deseadas
partido_ganador_municipio = partido_ganador_municipio.join(poblacion_comunidad, "comunidad") \
.select("comunidad", "municipio", "poblacion", "partido","votos","poblacion_total")

display(partido_ganador_municipio)


# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, expr

# Transformar los votos en filas para identificar el partido más votado
partidos_comunidad = df.select("comunidad",
    expr("stack(5, 'pp', pp, 'psoe', psoe, 'podemosiuequo', podemosiuequo, 'cs', cs, 'erccats', erccats) as (partido, votos)")
)

# Identificar el partido más votado en cada comunidad
windowPartidoComunidad = Window.partitionBy("comunidad").orderBy(col("votos").desc())
partido_ganador_comunidad = partidos_comunidad.withColumn("row", row_number().over(windowPartidoComunidad)) \
                                              .filter(col("row") == 1) \
                                              .drop("row")


# COMMAND ----------

# Calcular el porcentaje de población en el municipio más grande
partido_ganador_municipio = partido_ganador_municipio.withColumn("porcentaje_poblacion", 
                                                                 (col("poblacion") / col("poblacion_total")) * 100)

# Unir el partido ganador del municipio con el partido ganador de la comunidad
comparacion = partido_ganador_municipio.alias("municipio") \
               .join(partido_ganador_comunidad.alias("comunidad"), "comunidad") \
               .select("comunidad", 
                       "municipio.municipio", 
                       "municipio.partido", 
                       col("municipio.votos").alias("votos_municipio"), 
                       col("comunidad.partido").alias("partido_comunidad"), 
                       col("comunidad.votos").alias("votos_comunidad"),
                       "municipio.porcentaje_poblacion")

display(comparacion)


# COMMAND ----------

# MAGIC %md
# MAGIC Los municipios más grandes, independientemente de su porcentaje de población en la comunidad, tienden a ser representativos del partido dominante en la comunidad. Este efecto se observa tanto en comunidades donde los municipios más grandes tienen una influencia significativa como en aquellas donde representan una fracción menor. Sin embargo, en regiones con porcentajes muy pequeños, la coincidencia podría ser fortuita y necesitaría análisis adicionales.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8 Vamos a analizar los datos de los municipios grandes y de los municipios pequeños.
# MAGIC 9. Sacaremos la participación y el top 5 de partidos votados en los 20 municipios con más población
# MAGIC de España.
# MAGIC * Sacaremos la participación y el top 5 de partidos votados en los 20 primeros
# MAGIC * municipios con menos de 10000 habitantes de España.
# MAGIC * Comparemos resultados. ¿Cómo se comportan los diferentes municipios y partidos? ¿Tiene
# MAGIC que ver la participación con que gane un partido u otro? ¿Y la provincia o autonomía?

# COMMAND ----------

from pyspark.sql.functions import col, desc

# Seleccionar los 20 municipios más poblados
municipios_grandes = df.orderBy(col("poblacion").desc()).limit(20)

# Calcular la participación en municipios grandes
municipios_grandes = municipios_grandes.withColumn("participacion", (col("votantes") / col("censo")) * 100)

# Obtener el top 5 de partidos más votados en municipios grandes
top_partidos_grandes = municipios_grandes.select(
    "municipio", "provincia", "comunidad", "participacion", 
    "pp", "psoe", "podemosiuequo", "cs", "vox"
)
display(top_partidos_grandes)

# COMMAND ----------

# Filtrar municipios con menos de 10,000 habitantes y seleccionar los primeros 20
municipios_pequenos = df.filter(col("poblacion") < 10000).orderBy(col("poblacion").desc()).limit(20)

# Calcular la participación en municipios pequeños
municipios_pequenos = municipios_pequenos.withColumn("participacion", (col("votantes") / col("censo")) * 100)

# Obtener el top 5 de partidos más votados en municipios pequeños
top_partidos_pequenos = municipios_pequenos.select(
    "municipio", "provincia", "comunidad", "participacion", 
    "pp", "psoe", "podemosiuequo", "cs", "vox"
)
display(top_partidos_pequenos)

# COMMAND ----------

# MAGIC %md
# MAGIC Participación
# MAGIC Municipios grandes:
# MAGIC
# MAGIC Promedio de participación: 70.6%
# MAGIC La participación es alta en general, con municipios como Valencia y Madrid superando el 73%.
# MAGIC Municipios con menor participación: Palma de Mallorca (62%) y L'Hospitalet de Llobregat (65%).
# MAGIC Municipios pequeños:
# MAGIC
# MAGIC Promedio de participación: 70.5%
# MAGIC Similar a los municipios grandes, pero con una mayor dispersión.
# MAGIC Municipios con participación más alta: Buñol (78.8%) y Castalla (78.2%).
# MAGIC Municipios con menor participación: Zumaia (65%) y Ordizia (66.7%).
# MAGIC
# MAGIC La participación promedio es comparable entre municipios grandes y pequeños. Sin embargo, los municipios pequeños tienden a mostrar una mayor variabilidad en participación.

# COMMAND ----------

# MAGIC %md
# MAGIC Municipios grandes:
# MAGIC
# MAGIC PP: Dominante en la mayoría de los municipios grandes. Por ejemplo, obtiene mayoría absoluta en Madrid (696,804 votos) y Valencia (159,079 votos).
# MAGIC PSOE: En segundo lugar en muchos municipios grandes como Sevilla, Málaga y Bilbao.
# MAGIC Podemos-IU-Equo: Fuerte en algunas localidades, como Bilbao (50,083 votos) y Zaragoza (78,527 votos).
# MAGIC VOX: Baja representación general; su mayor desempeño está en Madrid, aunque sigue siendo marginal.
# MAGIC Municipios pequeños:
# MAGIC
# MAGIC PP: Sigue dominando en la mayoría de los municipios pequeños, pero con márgenes menores.
# MAGIC PSOE: Más competitivo en municipios pequeños, llegando a ser el partido más votado en localidades como Tocina (2,843 votos) y Mengíbar (2,256 votos).
# MAGIC Podemos-IU-Equo: Consigue un desempeño relevante en algunos municipios como Zumaia (1,317 votos) y Ordizia (1,129 votos).
# MAGIC Otros partidos: En municipios pequeños hay más fragmentación del voto hacia partidos locales o regionales.
# MAGIC
# MAGIC  El PP domina en ambos contextos, pero el PSOE es más competitivo en municipios pequeños. Además, la fragmentación del voto en municipios pequeños indica un mayor peso de partidos locales y minoritarios.

# COMMAND ----------

# MAGIC %md
# MAGIC Relación con provincias y autonomías
# MAGIC En Catalunya, partidos regionales como ERC (en municipios grandes como Barcelona) y otros partidos de izquierda como Podemos son más fuertes.
# MAGIC En el País Vasco, Podemos-IU-Equo y otros partidos minoritarios tienen más presencia que en otras regiones.
# MAGIC En Andalucía, el PP y PSOE se disputan las mayorías en municipios grandes y pequeños.
# MAGIC En Comunitat Valenciana, el PP domina tanto en grandes como pequeños municipios, aunque con fragmentación significativa en municipios pequeños.

# COMMAND ----------

# MAGIC %md
# MAGIC ###  10 En el caso actual la mayoría de respuestas se pueden plantear en entornos relacionales o pseudorelacionales. Si tuviésemos una versión turbo de las urnas de votación en la que cada vez que se produce una votación se generase un mensaje al respecto (obviamente no de quien vota ni a quién, sólo de un voto) para tener la participación en tiempo real, ¿podríamos considerar una base de datos NoSQL para almacenar la información? ¿Qué tipo de base de datos utilizaríamos y qué información se te ocurre que almacenaríamos? Debemos considerar que hay una mesa de votación por cada 500 votantes, por lo que asumiendo 30 millones de votantes y una participación del 100% tendríamos un total de 60.000 urnas emitiendo información de votación. Haciéndolo lineal en las 11 horas que suele durar la jornada de votaciones, hablamos de un máximo de 45.000 votos por minuto.

# COMMAND ----------

# MAGIC %md
# MAGIC Sí, una base de datos NoSQL sería una opción ideal para manejar este caso debido al volumen, velocidad y la estructura simple de los datos que generaría este sistema.
# MAGIC Para esto el sistema tener lo siguiente:
# MAGIC - Alta velocidad de escritura:
# MAGIC Manejar un flujo constante de 45,000 votos/minuto, con picos potenciales superiores.
# MAGIC - Distribución geográfica:
# MAGIC La información proviene de 60,000 urnas distribuidas en todo el país, lo que implica la necesidad de una infraestructura escalable y de baja latencia.
# MAGIC - Escalabilidad horizontal:
# MAGIC La base de datos debe crecer fácilmente agregando nodos a medida que aumente la participación o el número de votantes.
# MAGIC - Consulta en tiempo real:
# MAGIC Generar estadísticas como participación total, por comunidad, provincia o municipio sin afectar las operaciones de escritura.
# MAGIC - Simplitud del modelo de datos:
# MAGIC Cada mensaje contiene poca información (un voto registrado), lo que permite una estructura de datos sencilla.
# MAGIC
# MAGIC ###Tipo de Base de Datos NoSQL Ideal
# MAGIC **Base de datos de series temporales (Time Series Database):**
# MAGIC
# MAGIC Ejemplos: InfluxDB, Amazon Timestream, TimescaleDB.
# MAGIC Diseñadas para manejar flujos de datos con marca de tiempo.
# MAGIC Optimizadas para consultas de agregación y métricas en tiempo real.
# MAGIC Compresión eficiente de datos históricos.
# MAGIC
# MAGIC **Base de datos orientada a documentos:**
# MAGIC
# MAGIC Ejemplos: MongoDB, Couchbase.
# MAGIC Flexibles en cuanto al esquema, permiten agregar datos adicionales fácilmente.
# MAGIC Buen soporte para agregaciones y búsquedas por índices.
# MAGIC
# MAGIC **Base de datos de clave-valor con pub-sub:**
# MAGIC
# MAGIC Ejemplos: Redis Streams, Apache Kafka (para almacenamiento y procesamiento).
# MAGIC Ideales para manejar flujos de datos en tiempo real.
# MAGIC Redis puede actuar como almacenamiento temporal para analítica rápida, mientras que Kafka puede registrar los mensajes para análisis más extensos.
# MAGIC
# MAGIC Modelo de Datos
# MAGIC Cada mensaje puede contener la siguiente información mínima:
# MAGIC
# MAGIC `{
# MAGIC   "timestamp": "2024-11-24T09:15:34Z",    // Marca de tiempo
# MAGIC   "mesa_id": "123456",                    // Identificador único de la mesa
# MAGIC   "municipio_id": "ES-28079",             // Código del municipio
# MAGIC   "provincia": "Madrid",                  // Provincia de la mesa
# MAGIC   "comunidad": "Comunidad de Madrid",     // Comunidad Autónoma
# MAGIC   "mesa_participacion": 250,              // Participación acumulada en la mesa
# MAGIC   "total_municipio": 8000                 // Participación acumulada en el municipio
# MAGIC }`

# COMMAND ----------


