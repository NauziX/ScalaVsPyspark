# Comparativa de Procesamiento de Datos con Apache Spark: Scala vs PySpark

Este proyecto muestra una implementación paralela de ejercicios prácticos de procesamiento de datos usando Apache Spark, desarrollados en:

- ⚙️ **Scala + Spark** (ejecutado localmente con IntelliJ)
- ☁️ **Python + PySpark** (ejecutado en entorno remoto, con datos desde AWS)

El objetivo es **comparar los enfoques, ventajas y diferencias prácticas** de ambos lenguajes en contextos reales de trabajo con big data.

---

##  Estructura del repositorio

```
.
├── ExamenNauzet.scala      # Versión en Scala + Spark
├── ExamenNauzetTest.scala  # Tests automatizados de la versión Scala
├── TestInit.scala          # Infraestructura de testing (proporcionada)
├── build.sbt               # Configuración del proyecto Scala
├── ExamenNauzet.py         # Versión en Python + PySpark
├── AWS                     # Carpeta con imágenes configuracion en AWS
├── Scala vs Pyspark        # Carpeta con imágenes comparando código 
├── ventas.csv              # Archivo CSV de prueba
└── README.md               # Este archivo
```

---

##  Ejercicios Implementados

| Ejercicio | Descripción                                                                 |
|----------|-----------------------------------------------------------------------------|
| **1**     | Crear un DataFrame de estudiantes, filtrar por calificación, ordenar       |
| **2**     | UDF para determinar si un número es par o impar                            |
| **3**     | Join entre estudiantes y sus calificaciones + cálculo del promedio         |
| **4**     | Conteo de ocurrencias de palabras usando RDDs                              |
| **5**     | Lectura de archivo CSV y cálculo de ingresos por producto                  |

---

##  Comparativa: Scala + Spark vs PySpark

| Característica            | Scala + Spark                                       | Python + PySpark                                       |
|--------------------------|----------------------------------------------------|--------------------------------------------------------|
| **Lenguaje**             | Tipado estático, ejecutado en la JVM               | Dinámico, más flexible                                 |
| **Curva de Aprendizaje** | Más pronunciada, ideal para desarrolladores        | Más accesible para científicos de datos y analistas    |
| **Entorno de trabajo**   | Frecuente en desarrollo local(on-prem o cloud)     | Altamente integrado en plataformas cloud          |
| **Performance**          | Muy alto, especialmente en entornos Spark puros    | Excelente en clústeres distribuidos en la nube         |
| **Casos de Uso**         | Backend de datos, ETLs complejos,pipe empresariales| Ciencia de datos, prototipos, pipelines cloud-native   |

---

##  Infografía Visual

<img width="1720" alt="2025-04-11 13_41_17-" src="https://github.com/user-attachments/assets/2af766f8-9638-4c13-9a10-7acd14bafdac" />


---

##  Conclusiones

Después de trabajar con ambas implementaciones, puedo decir que **tanto Scala como Python se integran muy bien con Apache Spark** y permiten sacarle mucho partido a esta tecnología.

Aunque pueda parecer lo contrario, **personalmente me ha resultado más sencillo trabajar con Scala**, ya que ofrece de forma nativa algunas herramientas y estructuras que encajan muy bien con la lógica de Spark.

Aun así, **PySpark también tiene sus ventajas**, especialmente en entornos cloud o cuando se trabaja con equipos orientados a ciencia de datos.

En definitiva, como suele pasar en estos temas, **la mejor respuesta es: “depende”**.

Depende del proyecto, del equipo, del entorno de trabajo y del tipo de tareas que se quieran resolver. Lo importante es entender las fortalezas de cada enfoque y elegir en función de lo que se necesita.

---

##  Requisitos

### Para correr la versión en Scala
- IntelliJ IDEA
- Scala SDK 2.12.x o 2.13.x
- Apache Spark instalado localmente


### Para correr la versión en PySpark
- Python 3.8+
- PySpark instalado (`pip install pyspark`)
- Acceso a AWS S3

---

## Autor

**Nauzet**  
 Contacto: Nauzet.fdez@gmail.com

---

> Este repositorio busca servir como base de comparación y aprendizaje entre dos formas de trabajar con Spark. ¡Gracias por pasarte por aquí! 🚀
