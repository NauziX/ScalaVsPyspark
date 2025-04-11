from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col,avg,sum
from pyspark.sql.types import StringType

def main():
    builder: SparkSession.Builder = SparkSession.builder
    spark = builder.appName("ExamenNau").getOrCreate()

    ejercicio1(spark)
    ejercicio2(spark)
    ejercicio3(spark)
    ejercicio4(spark)
    ejercicio5(spark)

def ejercicio1(spark):
    
    """
    Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
    Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
    estudiantes (nombre, edad, calificación).
    Realiza las siguientes operaciones:

    Muestra el esquema del DataFrame.
    Filtra los estudiantes con una calificación mayor a 8.
    Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
    """

    datos = [
        ("Nauzet", 20, 9.5),
        ("David", 25, 10.0),
        ("Miguel", 30, 5.3),
        ("Marcos", 17, 6.2)
        ]

    columnas = ["nombre","edad","calificacion"]

    estudiantes = spark.createDataFrame(datos, columnas)
    estudiantes.printSchema()
    filtrados = estudiantes.filter(estudiantes.calificacion > 8)
    resultado = filtrados.select("nombre", "calificacion").orderBy("calificacion", ascending=False)
    resultado.show()
    


def ejercicio2(spark):
    
    """
    Ejercicio 2: UDF (User Defined Function)
    Pregunta: Define una función que determine si un número es par o impar.
    Aplica esta función a una columna de un DataFrame que contenga una lista de números.
    """

    numeros = [(1,),(2,),(3,),(4,),(5,)]
    columna = ["numero"]
    medataf = spark.createDataFrame(numeros, columna)
    es_par_udf = udf(lambda x: "es par" if x % 2 == 0 else "es impar", StringType())
    medataf = medataf.withColumn("Par o impar", es_par_udf(col("numero")))
    medataf.show()


def ejercicio3(spark):
    
    """
    Pregunta: Dado dos DataFrames,
    uno con información de estudiantes (id, nombre)
    y otro con calificaciones (id_estudiante, asignatura, calificacion),
    realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
    """
    
    dato1 = [(1,"nauzet"),(2,"pepe"),(3,"jose"),(4,"marco")]
    dato1_colum = ["id","nombre"]
    estudiantes = spark.createDataFrame(dato1, dato1_colum)
    
    dato2 = [(1, "Matemáticas", 8.0),
            (1, "Lengua", 7.5),
            (2, "Matemáticas", 9.0),
            (2, "Lengua", 9.4),
            (3, "Matemáticas", 6.0),
            (3, "Lengua", 7.0),
            (4, "Matemáticas", 8.5),
            (4, "Lengua", 9.5)]
    
    dato2_colum = ["id_estudiantes","asignatura","calificacion"]
    calificaciones = spark.createDataFrame(dato2, dato2_colum)

    medataf = estudiantes.join(calificaciones, estudiantes["id"] == calificaciones["id_estudiantes"])
    resultado = medataf.groupBy("nombre").agg(avg("calificacion").alias("promedio"))
    resultado.show()

def ejercicio4(spark):
    
    """
    Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
    """
    palabras = ["scala", "spark", "scala", "big", "data", "spark", "spark"]
    rdd_pala = spark.sparkContext.parallelize(palabras)
    result =  rdd_pala.map(lambda palabra: (palabra, 1)).reduceByKey(lambda a, b: a + b)
    result.collect()
    for palabra, cantidad in result.collect():
        print(palabra, cantidad)


def ejercicio5(spark):
    
    """
    Ejercicio 5: Procesamiento de archivos
    Pregunta: Carga un archivo CSV que contenga información sobre
    ventas (id_venta, id_producto, cantidad, precio_unitario)
    y calcula el ingreso total (cantidad * precio_unitario) por producto.
    """
    df = spark.read.option("header", True).option("inferSchema", True).csv("s3://pysparknau/ventas.csv" )
    df = df.withColumn("ingreso", col("cantidad") * col("precio_unitario"))
    resultado = df.groupBy("id_producto").agg(sum("ingreso").alias("total_ingreso"))
    resultado.show()

if __name__ == "__main__":
    main()
