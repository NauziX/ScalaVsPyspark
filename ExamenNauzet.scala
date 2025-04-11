package app
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._


object ExamenNauzet {

  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   estudiantes (nombre, edad, calificación).
   Realiza las siguientes operaciones:

   Muestra el esquema del DataFrame.
   Filtra los estudiantes con una calificación mayor a 8.
   Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */

  def ejercicio1(estudiantes: DataFrame)(implicit spark:SparkSession): DataFrame = {
    import spark.implicits._
    estudiantes.filter($"calificacion" > 8).orderBy($"calificacion".desc).select("nombre","calificacion")

  }

  /**Ejercicio 2: UDF (User Defined Function)
   Pregunta: Define una función que determine si un número es par o impar.
   Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */

  def ejercicio2(numeros: DataFrame)(spark:SparkSession): DataFrame =  {
    import spark.implicits._
    val esPar = udf((num:Int) => if (num % 2 == 0)"es par" else "es impar" )
    numeros.withColumn("Par o Impar",esPar($"numero"))
  }

  /**Ejercicio 3: Joins y agregaciones
   Pregunta: Dado dos DataFrames,
   uno con información de estudiantes (id, nombre)
   y otro con calificaciones (id_estudiante, asignatura, calificacion),
   realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */

  def ejercicio3(estudiantes1: DataFrame , calificaciones: DataFrame): DataFrame = {
    val dfJoin = estudiantes1.join(calificaciones, estudiantes1("id") === calificaciones("id_estudiante"))
    dfJoin.groupBy("nombre").agg(avg("calificacion").as("promedio"))

  }

  /**Ejercicio 4: Uso de RDDs
   Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

   */

  def ejercicio4(palabras: List[String])(spark:SparkSession): RDD[(String, Int)] = {
    val sc =  spark.sparkContext
    val palabraCount = sc.parallelize(palabras)
    palabraCount.map(palabra => (palabra,1)).reduceByKey(_+_)

  }
  /**
   Ejercicio 5: Procesamiento de archivos
   Pregunta: Carga un archivo CSV que contenga información sobre
   ventas (id_venta, id_producto, cantidad, precio_unitario)
   y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */
  def ejercicio5(ventas: DataFrame)(spark:SparkSession): DataFrame = {
    val dfConIngreso = ventas.withColumn("ingreso", col("cantidad") * col("precio_unitario"))
    val resumen = dfConIngreso
      .groupBy("id_producto")
      .agg(sum("ingreso").as("total_ingreso"))
    resumen

  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Examen Final Nauzet")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //Dataframe ejercicio1

    val estudiantes = Seq(("Nauzet",20,9.5),("David",25,10.0),("Miguel",30,5.3),("Marcos",17,6.2)).toDF("nombre","edad","calificacion")
    val result1 = ejercicio1(estudiantes)(spark)

    //Dataframe ejercicio2

    val numeros = Seq(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15).toDF("numero")
    val result2 = ejercicio2(numeros)(spark)

    //Dataframe ejercicio3

    val estudiantes1 = Seq(
      (1, "Luis"),
      (2, "Ana"),
      (3, "Carlos"),
      (4, "Elena")
    ).toDF("id", "nombre")

    val calificaciones = Seq(
      (1, "Matemáticas", 8.0),
      (1, "Lengua", 7.5),
      (2, "Matemáticas", 9.0),
      (2, "Lengua", 9.4),
      (3, "Matemáticas", 6.0),
      (3, "Lengua", 7.0),
      (4, "Matemáticas", 8.5)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    val result3 = ejercicio3(estudiantes1,calificaciones)

    //Dataframe ejercicio4
    val palabras = List("scala", "spark", "scala", "big", "data", "spark", "spark")
    val result4 = ejercicio4(palabras)(spark)

    //Cargar datos CSV
    val ventas = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Users\\Nauzet\\Scala\\MiPrimerSparkApp\\src\\main\\scala\\app\\data\\ventas.csv")
    val result5 = ejercicio5(ventas)(spark)


    result1.show()
    result2.show()
    result3.show()
    result4.collect().foreach(println)
    result5.show()

    spark.stop()
  }
}