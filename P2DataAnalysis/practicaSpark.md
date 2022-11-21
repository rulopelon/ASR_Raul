# Prática spark
## Objetivo
El objetivo de esta práctica es realizar el procesado de un fichero de logs para poder
## Práctica
### Conexión
El primer paso para realizar la práctica es establecer la conexión con el cluster de spark


```python
from pyspark.sql import SparkSession
from  pyspark.sql.functions import col, concat_ws,split, explode ,desc, to_timestamp,from_unixtime,unix_timestamp, translate, create_map, lit,udf,StringType,map_keys
from datetime import datetime
from itertools import chain

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()
```

    22/11/21 16:44:44 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).


### Lectura
Una vez que se ha establecido la conexión con el cluster de spark, es necesario cargar el archivo de texto que se va a tratar.
Para esta práctica se va a leer como un dataframe


```python
ficheroDataframe = spark.read.text("logs.txt")
ficheroDataFrameRenamed = ficheroDataframe.withColumnRenamed("value","datos")
# Se muestran los primeros 20 elementos
ficheroDataFrameRenamed.show()
```

    [Stage 0:>                                                          (0 + 1) / 1]

    +--------------------+
    |               datos|
    +--------------------+
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    |01-11-2022 13:24:...|
    +--------------------+
    only showing top 20 rows
    


                                                                                    

### Preprocesado

Una vez que se ha cargado el fichero, será necesario dividir la columna por el carácter "-"


```python
ficheroDataFrameRenamed.filter(col("datos").isNull()).show()
```

    [Stage 1:>                                                          (0 + 1) / 1]

    +-----+
    |datos|
    +-----+
    +-----+
    


                                                                                    

Además comprobamos que todas las filas tienen el carácter por el que vamos a separar el string


```python
ficheroDataFrameRenamed.filter(~ col("Datos").contains(" - ")).show()
```

    +-----+
    |datos|
    +-----+
    +-----+
    


                                                                                    

Ahora que ya se sabe que los datos de entrada tienen el formato adecuado se puede pasar a la división


```python
ficheroDataFrameSplit = ficheroDataFrameRenamed.select(split(col("datos")," - "))
ficheroDataFrameSplit = ficheroDataFrameSplit.withColumnRenamed("split(datos,  - , -1)","datos")

ficheroDataFrameSplit.printSchema()

```

    root
     |-- datos: array (nullable = true)
     |    |-- element: string (containsNull = true)
    


Se construye el dataframe 


```python
dataFrameDatos= ficheroDataFrameSplit.select(col("datos").getItem(0),col("datos").getItem(1),col("datos").getItem(2),col("datos").getItem(3))
# Se renombran las columnas
dataFrameDatos = dataFrameDatos.withColumnRenamed("datos[0]","Fecha")
dataFrameDatos = dataFrameDatos.withColumnRenamed("datos[1]","Log")
dataFrameDatos = dataFrameDatos.withColumnRenamed("datos[2]","Module")
dataFrameDatos = dataFrameDatos.withColumnRenamed("datos[3]","Info")
dataFrameDatos.show()

```

    +-------------------+--------+------+--------------------+
    |              Fecha|     Log|Module|                Info|
    +-------------------+--------+------+--------------------+
    |01-11-2022 13:24:13|    INFO|pandas|Aliquam amet adip...|
    |01-11-2022 13:24:17| WARNING| spark|Consectetur velit...|
    |01-11-2022 13:24:20| WARNING|pandas|Porro etincidunt ...|
    |01-11-2022 13:24:22|   ERROR|pandas|Tempora modi quiq...|
    |01-11-2022 13:24:24| WARNING| spark|Quiquia etincidun...|
    |01-11-2022 13:24:25|    INFO| spark|Non est porro por...|
    |01-11-2022 13:24:26|CRITICAL|python|Porro labore eius...|
    |01-11-2022 13:24:28|CRITICAL| numpy|Est ut tempora se...|
    |01-11-2022 13:24:29|    INFO| spark|Sit dolorem dolor...|
    |01-11-2022 13:24:30|CRITICAL| spark|Etincidunt aliqua...|
    |01-11-2022 13:24:31|   DEBUG|python|Aliquam sed porro...|
    |01-11-2022 13:24:32|CRITICAL| numpy|Numquam dolor ips...|
    |01-11-2022 13:24:33|CRITICAL|python|Amet neque est ip...|
    |01-11-2022 13:24:34|   DEBUG|python|Eius ut tempora i...|
    |01-11-2022 13:24:35| WARNING| spark|Quiquia numquam s...|
    |01-11-2022 13:24:36|   ERROR| numpy|Quisquam adipisci...|
    |01-11-2022 13:24:37|CRITICAL| spark|Voluptatem dolor ...|
    |01-11-2022 13:24:38|CRITICAL| spark| Dolor neque ut non.|
    |01-11-2022 13:24:39|   DEBUG| numpy|Tempora adipisci ...|
    |01-11-2022 13:24:40|   ERROR| numpy|Velit numquam dol...|
    +-------------------+--------+------+--------------------+
    only showing top 20 rows
    


Ahora se tiene el dataframe con los datos separados por columnas, se tiene que transformar la fecha a timestamp


```python
dataFrameDatos = dataFrameDatos.withColumn("Fecha",to_timestamp(col("Fecha"),"dd-MM-yyyy HH:mm:ss"))
dataFrameDatos.show()
dataFrameDatos.printSchema()
```

    +-------------------+--------+------+--------------------+
    |              Fecha|     Log|Module|                Info|
    +-------------------+--------+------+--------------------+
    |2022-11-01 13:24:13|    INFO|pandas|Aliquam amet adip...|
    |2022-11-01 13:24:17| WARNING| spark|Consectetur velit...|
    |2022-11-01 13:24:20| WARNING|pandas|Porro etincidunt ...|
    |2022-11-01 13:24:22|   ERROR|pandas|Tempora modi quiq...|
    |2022-11-01 13:24:24| WARNING| spark|Quiquia etincidun...|
    |2022-11-01 13:24:25|    INFO| spark|Non est porro por...|
    |2022-11-01 13:24:26|CRITICAL|python|Porro labore eius...|
    |2022-11-01 13:24:28|CRITICAL| numpy|Est ut tempora se...|
    |2022-11-01 13:24:29|    INFO| spark|Sit dolorem dolor...|
    |2022-11-01 13:24:30|CRITICAL| spark|Etincidunt aliqua...|
    |2022-11-01 13:24:31|   DEBUG|python|Aliquam sed porro...|
    |2022-11-01 13:24:32|CRITICAL| numpy|Numquam dolor ips...|
    |2022-11-01 13:24:33|CRITICAL|python|Amet neque est ip...|
    |2022-11-01 13:24:34|   DEBUG|python|Eius ut tempora i...|
    |2022-11-01 13:24:35| WARNING| spark|Quiquia numquam s...|
    |2022-11-01 13:24:36|   ERROR| numpy|Quisquam adipisci...|
    |2022-11-01 13:24:37|CRITICAL| spark|Voluptatem dolor ...|
    |2022-11-01 13:24:38|CRITICAL| spark| Dolor neque ut non.|
    |2022-11-01 13:24:39|   DEBUG| numpy|Tempora adipisci ...|
    |2022-11-01 13:24:40|   ERROR| numpy|Velit numquam dol...|
    +-------------------+--------+------+--------------------+
    only showing top 20 rows
    
    root
     |-- Fecha: timestamp (nullable = true)
     |-- Log: string (nullable = true)
     |-- Module: string (nullable = true)
     |-- Info: string (nullable = true)
    


Por último va a ser necesario realilzar un mapeo entre el nivel de incidencia y in valor numérico previamente establecido


```python
log_mapping = {
    'CRITICAL': 50,
    'ERROR': 40,
    'WARNING': 30,
    'INFO': 20,
    'DEBUG': 10, 
    'NOTSET': 0}
log_mapping_spark = create_map([lit(x) for x in chain(*log_mapping.items())])

dataFrameDatos = dataFrameDatos.withColumn("Map",log_mapping_spark[col("Log")])
dataFrameDatos.show()

```

    +-------------------+--------+------+--------------------+---+
    |              Fecha|     Log|Module|                Info|Map|
    +-------------------+--------+------+--------------------+---+
    |2022-11-01 13:24:13|    INFO|pandas|Aliquam amet adip...| 20|
    |2022-11-01 13:24:17| WARNING| spark|Consectetur velit...| 30|
    |2022-11-01 13:24:20| WARNING|pandas|Porro etincidunt ...| 30|
    |2022-11-01 13:24:22|   ERROR|pandas|Tempora modi quiq...| 40|
    |2022-11-01 13:24:24| WARNING| spark|Quiquia etincidun...| 30|
    |2022-11-01 13:24:25|    INFO| spark|Non est porro por...| 20|
    |2022-11-01 13:24:26|CRITICAL|python|Porro labore eius...| 50|
    |2022-11-01 13:24:28|CRITICAL| numpy|Est ut tempora se...| 50|
    |2022-11-01 13:24:29|    INFO| spark|Sit dolorem dolor...| 20|
    |2022-11-01 13:24:30|CRITICAL| spark|Etincidunt aliqua...| 50|
    |2022-11-01 13:24:31|   DEBUG|python|Aliquam sed porro...| 10|
    |2022-11-01 13:24:32|CRITICAL| numpy|Numquam dolor ips...| 50|
    |2022-11-01 13:24:33|CRITICAL|python|Amet neque est ip...| 50|
    |2022-11-01 13:24:34|   DEBUG|python|Eius ut tempora i...| 10|
    |2022-11-01 13:24:35| WARNING| spark|Quiquia numquam s...| 30|
    |2022-11-01 13:24:36|   ERROR| numpy|Quisquam adipisci...| 40|
    |2022-11-01 13:24:37|CRITICAL| spark|Voluptatem dolor ...| 50|
    |2022-11-01 13:24:38|CRITICAL| spark| Dolor neque ut non.| 50|
    |2022-11-01 13:24:39|   DEBUG| numpy|Tempora adipisci ...| 10|
    |2022-11-01 13:24:40|   ERROR| numpy|Velit numquam dol...| 40|
    +-------------------+--------+------+--------------------+---+
    only showing top 20 rows
    


Se comprueba que las columnas tienen el formato que esperamos


```python
dataFrameDatos.printSchema()
```

    root
     |-- Fecha: timestamp (nullable = true)
     |-- Log: string (nullable = true)
     |-- Module: string (nullable = true)
     |-- Info: string (nullable = true)
     |-- Map: integer (nullable = true)
    


Como era de esperar no se ha encontrado ningún valor a null, por lo tanto se puede continuar con la práctica

### Filtrado por módulo
Con el dataframe preprocesado como se desea, se va a comenzar a extraer información.
En un inicio se van a extraer las filas que coincidan con un valor de módulo.


```python
modulo = "spark"
dataFrameDatos = dataFrameDatos.filter(modulo== col("Module"))
dataFrameDatos.show()
```

    +-------------------+--------+------+--------------------+---+
    |              Fecha|     Log|Module|                Info|Map|
    +-------------------+--------+------+--------------------+---+
    |2022-11-01 13:24:17| WARNING| spark|Consectetur velit...| 30|
    |2022-11-01 13:24:24| WARNING| spark|Quiquia etincidun...| 30|
    |2022-11-01 13:24:25|    INFO| spark|Non est porro por...| 20|
    |2022-11-01 13:24:29|    INFO| spark|Sit dolorem dolor...| 20|
    |2022-11-01 13:24:30|CRITICAL| spark|Etincidunt aliqua...| 50|
    |2022-11-01 13:24:35| WARNING| spark|Quiquia numquam s...| 30|
    |2022-11-01 13:24:37|CRITICAL| spark|Voluptatem dolor ...| 50|
    |2022-11-01 13:24:38|CRITICAL| spark| Dolor neque ut non.| 50|
    |2022-11-01 13:24:51|   DEBUG| spark|Modi est modi non...| 10|
    |2022-11-01 13:24:51| WARNING| spark|Magnam numquam ut...| 30|
    |2022-11-01 13:24:51|   DEBUG| spark|Modi quiquia cons...| 10|
    |2022-11-01 13:24:51|    INFO| spark|Neque adipisci ma...| 20|
    |2022-11-01 13:24:51| WARNING| spark|Velit numquam neq...| 30|
    |2022-11-01 13:24:51|   DEBUG| spark|Quaerat magnam se...| 10|
    |2022-11-01 13:24:51| WARNING| spark|Est modi sit nequ...| 30|
    |2022-11-01 13:24:51|   DEBUG| spark|Sit ipsum quiquia...| 10|
    |2022-11-01 13:24:51| WARNING| spark|Quisquam amet adi...| 30|
    |2022-11-01 13:24:51|   ERROR| spark|Porro dolorem sit...| 40|
    |2022-11-01 13:24:51|   DEBUG| spark|Dolor sed etincid...| 10|
    |2022-11-01 13:24:51| WARNING| spark|Non magnam volupt...| 30|
    +-------------------+--------+------+--------------------+---+
    only showing top 20 rows
    


### Filtrado por palabra en string
En este apartado se va filtrar si una palabra se encuentra en el campo de información


```python
palabra = "Porro"
dataFrameDatos = dataFrameDatos.filter(col("Info").contains(palabra))
dataFrameDatos.show()
```

    +-------------------+--------+------+--------------------+---+
    |              Fecha|     Log|Module|                Info|Map|
    +-------------------+--------+------+--------------------+---+
    |2022-11-01 13:24:51|   ERROR| spark|Porro dolorem sit...| 40|
    |2022-11-01 13:24:51|CRITICAL| spark|Porro eius consec...| 50|
    |2022-11-01 13:24:51|   DEBUG| spark|Porro sed dolore ...| 10|
    |2022-11-01 13:24:51|    INFO| spark|Porro tempora qui...| 20|
    |2022-11-01 13:24:51|    INFO| spark|Porro dolorem dol...| 20|
    |2022-11-01 13:24:51|   ERROR| spark|Porro consectetur...| 40|
    |2022-11-01 13:24:51| WARNING| spark|Porro est neque d...| 30|
    |2022-11-01 13:24:51|   ERROR| spark|Porro ut eius sed...| 40|
    |2022-11-01 13:24:51|    INFO| spark|Porro est numquam...| 20|
    |2022-11-01 13:24:51| WARNING| spark|Porro quaerat ame...| 30|
    |2022-11-01 13:24:51| WARNING| spark|Porro ut tempora ...| 30|
    |2022-11-01 13:24:51| WARNING| spark|Porro dolorem con...| 30|
    +-------------------+--------+------+--------------------+---+
    


### Filtrado por input de palabra
Se van a mostrar las filas que tengan un nivel de incidencia igual o superior que el nivel solicitado. De esta forma si se introduce como nivel mínimo "Warning", se deberían de mostrar las filas con niveles "Warning", "Error" y "Critical"


```python
nivel = "WARNING"

# Primero se obtiene el valor numérico para el nivel
valor = log_mapping[nivel]

dataFrameDatos = dataFrameDatos.filter(valor <= col("Map"))
dataFrameDatos.show()
```

    +-------------------+--------+------+--------------------+---+
    |              Fecha|     Log|Module|                Info|Map|
    +-------------------+--------+------+--------------------+---+
    |2022-11-01 13:24:51|   ERROR| spark|Porro dolorem sit...| 40|
    |2022-11-01 13:24:51|CRITICAL| spark|Porro eius consec...| 50|
    |2022-11-01 13:24:51|   ERROR| spark|Porro consectetur...| 40|
    |2022-11-01 13:24:51| WARNING| spark|Porro est neque d...| 30|
    |2022-11-01 13:24:51|   ERROR| spark|Porro ut eius sed...| 40|
    |2022-11-01 13:24:51| WARNING| spark|Porro quaerat ame...| 30|
    |2022-11-01 13:24:51| WARNING| spark|Porro ut tempora ...| 30|
    |2022-11-01 13:24:51| WARNING| spark|Porro dolorem con...| 30|
    +-------------------+--------+------+--------------------+---+
    


### Filtrado por fecha
Se filtra para obtener todas las entradas del log posteriores al valor de fecha establecido


```python
after = datetime.strptime('11/01/22 13:24:41', '%m/%d/%y %H:%M:%S')

dataFrameDatos = dataFrameDatos.filter(col("Fecha") >(after)) 
dataFrameDatos.show()
```

    +-------------------+--------+------+--------------------+---+
    |              Fecha|     Log|Module|                Info|Map|
    +-------------------+--------+------+--------------------+---+
    |2022-11-01 13:24:51|   ERROR| spark|Porro dolorem sit...| 40|
    |2022-11-01 13:24:51|CRITICAL| spark|Porro eius consec...| 50|
    |2022-11-01 13:24:51|   ERROR| spark|Porro consectetur...| 40|
    |2022-11-01 13:24:51| WARNING| spark|Porro est neque d...| 30|
    |2022-11-01 13:24:51|   ERROR| spark|Porro ut eius sed...| 40|
    |2022-11-01 13:24:51| WARNING| spark|Porro quaerat ame...| 30|
    |2022-11-01 13:24:51| WARNING| spark|Porro ut tempora ...| 30|
    |2022-11-01 13:24:51| WARNING| spark|Porro dolorem con...| 30|
    +-------------------+--------+------+--------------------+---+
    

