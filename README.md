# Comparativa de Procesamiento de Datos con Apache Spark: Scala vs PySpark

Este proyecto muestra una implementación paralela de ejercicios prácticos de procesamiento de datos usando Apache Spark, desarrollados en:

- ⚙️ **Scala + Spark** (ejecutado localmente con IntelliJ)
- ☁️ **Python + PySpark** (ejecutado en entorno remoto, con datos desde AWS S3)

El objetivo es **comparar los enfoques, ventajas y diferencias prácticas** de ambos lenguajes en contextos reales de trabajo con big data.

---

## 📁 Estructura del repositorio

```
.
├── ExamenNauzet.scala      # Versión en Scala + Spark
├── ExamenNauzet.py         # Versión en Python + PySpark
├── AWS                     # Carpeta con imágenes configuracion en AWS
├── Scala vs Pyspark        # Carpeta con imágenes comparando código 
├── ventas.csv              # Archivo CSV de prueba
└── README.md               # Este archivo
```

---

## ✅ Ejercicios Implementados

| Ejercicio | Descripción                                                                 |
|----------|-----------------------------------------------------------------------------|
| **1**     | Crear un DataFrame de estudiantes, filtrar por calificación, ordenar       |
| **2**     | UDF para determinar si un número es par o impar                            |
| **3**     | Join entre estudiantes y sus calificaciones + cálculo del promedio         |
| **4**     | Conteo de ocurrencias de palabras usando RDDs                              |
| **5**     | Lectura de archivo CSV y cálculo de ingresos por producto                  |

---

## ⚖️ Comparativa: Scala + Spark vs PySpark

| Característica            | Scala + Spark                                       | Python + PySpark                                       |
|--------------------------|----------------------------------------------------|--------------------------------------------------------|
| **Lenguaje**             | Tipado estático, ejecutado en la JVM               | Dinámico, más flexible                                 |
| **Curva de Aprendizaje** | Más pronunciada, ideal para desarrolladores        | Más accesible para científicos de datos y analistas    |
| **Entorno**              | Local (IntelliJ IDEA), ficheros locales            | Cloud-ready (VSCode, S3), más escalable                |
| **Performance**          | Muy alto, especialmente en entornos Spark puros    | Excelente en clústeres distribuidos en la nube         |
| **Casos de Uso**         | Producción backend, procesamiento batch complejo   | Ciencia de datos, prototipos, pipelines cloud-native   |

---

## 📷 Infografía Visual



---

## 📌 Conclusiones

- Ambas implementaciones permiten explotar la potencia de Apache Spark de forma efectiva.
- **Scala + Spark** destaca en entornos productivos robustos y estructuras empresariales grandes.
- **PySpark** es más usado en ciencia de datos, aprendizaje automático, y trabajos en la nube.
- La elección de la tecnología depende del equipo, la infraestructura y el flujo de trabajo.

---

## 📦 Requisitos

### Para correr la versión en Scala
- IntelliJ IDEA
- Scala SDK 2.12.x o 2.13.x
- Apache Spark instalado localmente


### Para correr la versión en PySpark
- Python 3.8+
- PySpark instalado (`pip install pyspark`)
- Acceso a AWS S3 (para lectura del CSV remoto)

---

## 🧑‍💻 Autor

**Nauzet**  
📧 Contacto: Nauzet.fdez@gmail.com

---

> Este repositorio busca servir como base de comparación y aprendizaje entre dos formas de trabajar con Spark. ¡Gracias por pasarte por aquí! 🚀
