import app.ExamenNauzet._
import utils.TestInit

class ExamenNauzetTest extends TestInit {

  import spark.implicits._

  "Ejercicio 1" should "filtrar y ordenar estudiantes con calificacion > 8" in {
    val estudiantes = Seq(
      ("Luis", 20, 9.5),
      ("Ana", 22, 6.0),
      ("Carlos", 25, 10.0)
    ).toDF("nombre", "edad", "calificacion")

    val resultado = ejercicio1(estudiantes)

    resultado.count() shouldBe 2
    resultado.columns should contain allOf ("nombre", "calificacion")
    resultado.collect().map(_.getString(0)).toSeq shouldBe Seq("Carlos", "Luis")
  }

  "Ejercicio 2" should "añadir columna indicando si el número es par o impar" in {
    val numeros = Seq(1, 2, 3, 4, 5).toDF("numero")
    val resultado = ejercicio2(numeros)(spark)

    resultado.columns should contain allOf ("numero", "Par o Impar")
    resultado.count() shouldBe 5

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
      (1, "Luis"),
      (2, "Ana"),
      (3, "Carlos")
    ).toDF("id", "nombre")

    val calificaciones = Seq(
      (1, "Matemáticas", 8.0),
      (1, "Lengua", 7.0),
      (2, "Matemáticas", 9.0),
      (3, "Lengua", 6.0),
      (3, "Historia", 8.0)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    val resultado = ejercicio3(estudiantes1, calificaciones)

    resultado.columns should contain allOf ("nombre", "promedio")
    resultado.count() shouldBe 3

    val esperado = Map(
      "Luis" -> 7.5,
      "Ana" -> 9.0,
      "Carlos" -> 7.0
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
    val ventas = Seq(
      (1, "A", 2, 10.0),
      (2, "B", 1, 20.0),
      (3, "A", 3, 10.0),
      (4, "C", 5, 5.0)
    ).toDF("id_venta", "id_producto", "cantidad", "precio_unitario")

    val resultado = ejercicio5(ventas)(spark)

    val esperado = Map(
      "A" -> 50.0,
      "B" -> 20.0,
      "C" -> 25.0
    )

    val resultadoMap = resultado.collect().map(f => f.getString(0) -> f.getDouble(1)).toMap

    resultado.columns should contain allOf ("id_producto", "total_ingreso")
    resultado.count() shouldBe 3
    resultadoMap shouldBe esperado
  }


}


