import app.ExamenNauzet._
import utils.TestInit

class ExamenNauzetTest extends TestInit {

  import spark.implicits._

  "Ejercicio 1" should "filtrar y ordenar estudiantes con calificacion > 8" in {
    val estudiantes = Seq(
      ("Nauzet", 20, 9.5),
      ("David", 22, 6.0),
      ("Miguel", 25, 10.0)
    ).toDF("nombre", "edad", "calificacion")

    val resultado = ejercicio1(estudiantes)

    resultado.collect().map(_.getString(0)).toSeq shouldBe Seq("Miguel", "Nauzet")
  }

  "Ejercicio 2" should "añadir columna indicando si el número es par o impar" in {
    val numeros = Seq(1, 2, 3, 4, 5).toDF("numero")
    val resultado = ejercicio2(numeros)(spark)

      val esperado = Map(
        1 -> "es impar",
        2 -> "es par",
        3 -> "es impar",
        4 -> "es par",
        5 -> "es impar"
      )

      resultado.collect().foreach { fila =>
        val num = fila.getInt(0)
        val etiqueta = fila.getString(1)
        etiqueta shouldBe esperado(num)
      }
  }

  "Ejercicio 3" should "unir y calcular promedio de calificaciones por estudiante" in {
    val estudiantes1 = Seq(
      (1, "Nauzet"),
      (2, "Aday"),
      (3, "Eche")
    ).toDF("id", "nombre")

    val calificaciones = Seq(
      (1, "Matemáticas", 8.0),
      (1, "Lengua", 7.0),
      (2, "Matemáticas", 9.0),
      (2, "Lengua", 9.0),
      (3, "Matemáticas", 6.0),
      (3, "Lengua", 8.0)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    val resultado = ejercicio3(estudiantes1, calificaciones)
    val esperado = Map(
      "Nauzet" -> 7.5,
      "Aday" -> 9.0,
      "Eche" -> 7.0
    )

    resultado.collect().foreach { fila =>
      val nombre = fila.getString(0)
      val promedio = fila.getDouble(1)
      promedio shouldBe esperado(nombre)
    }
  }

  "Ejercicio 4" should "contar ocurrencias de palabras en un RDD" in {
    val palabras = List("spark", "scala", "spark", "big", "data", "spark")

    val resultado = ejercicio4(palabras)(spark)

    val esperado = Map(
      "spark" -> 3,
      "scala" -> 1,
      "big" -> 1,
      "data" -> 1
    )

    resultado.collect().toMap shouldBe esperado
  }


  "Ejercicio 5" should "calcular ingreso total por producto desde CSV de ventas" in {
    val ventas = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\Nauzet\\Scala\\MiPrimerSparkApp\\src\\main\\scala\\app\\data\\ventas.csv")

    val out = ejercicio5(ventas)(spark).collect().map(x => (x.getInt(0), x.getDouble(1)))

    out.toList shouldBe List((108,486.0), (101,460.0), (103,280.0), (107,396.0), (102,405.0), (109,540.0), (105,570.0), (110,494.0), (106,425.0), (104,800.0))

  }


}


