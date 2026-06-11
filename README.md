# Data Lakehouse: diseño e implementación de una arquitectura distribuida multimotor sobre almacenamiento de objetos

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-00D2FF?style=for-the-badge&logo=apache&logoColor=white)
![Trino](https://img.shields.io/badge/Trino-DD00A1?style=for-the-badge&logo=trino&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C7202C?style=for-the-badge&logo=minio&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)

Este repositorio contiene la implementación completa de un **Data Lakehouse** distribuido y multimotor, diseñado como Trabajo de Fin de Grado. La arquitectura realiza la ingesta, almacenamiento y transformación de conjuntos de datos históricos de la NBA (*box scores* y datos de salarios en formato CSV). 

La infraestructura se basa en la implementación de una **Arquitectura Medallón** (capas Bronce, Plata y Oro), que permite trabajar con los datos de forma progresiva. Para ello, se usa el **formato de tabla abierto** (Apache Iceberg) desplegado sobre almacenamiento de objetos (MinIO).

Todo el *pipeline* de datos está desacoplado y utiliza las herramientas estándar del *Modern Data Stack*: la ingesta se gestiona con Apache Kafka, el procesamiento pesado se delega a Apache Spark, y las agregaciones finales para *Business Intelligence* son ejecutadas por el motor SQL distribuido Trino, con Apache Airflow orquestando el ciclo de vida completo.

---

## 1. Arquitectura y Tecnologías

El proyecto implementa una arquitectura Medallón (Bronce, Plata, Oro) completamente contenerizada:

- **Almacenamiento & Formato:** MinIO (Object Storage S3) + Apache Iceberg.
- **Ingesta (Capa Bronce):** Apache Kafka (registro de partidos) y carga de ficheros estáticos CSV (salarios).
- **Procesamiento ETL (Capa Plata):** Apache Spark (PySpark) para limpieza y *Fuzzy Matching*.
- **Datamarts (Capa Oro) y Orquestación:** Apache Airflow coordina la ejecución de modelos analíticos sobre Trino (Motor SQL).
- **Visualización (BI):** Metabase.

---

## 2. Despliegue de la Infraestructura (Quick Start)

Los siguientes comandos se pueden ejecutar directamente desde la terminal de VS Code (PowerShell o CMD) en Windows, Mac o Linux.

**Requisitos previos:**
* Docker y Docker Compose instalados.
* Mínimo de 8 GB de RAM asignados a Docker (recomendado 12 GB).
* Puertos libres: `8085` (Airflow), `8080` (Trino), `9000/9001` (MinIO), `3000` (Metabase).

### Levantar los servicios
Ejecuta el siguiente comando en la raíz del proyecto para descargar las imágenes y levantar todo el clúster. *(Nota: La creación de los buckets internos en MinIO está automatizada y ocurrirá sola al arrancar).*

```bash
docker-compose up -d
```

---

## 3. Ejecución del Pipeline de Datos

Para procesar la información, debes seguir este orden estricto de ejecución desde tu terminal de VS Code:

### Paso 1: Inicialización de Tablas (Iceberg)
Ejecuta estos dos comandos para indicarle a Spark que cree las tablas vacías con el formato Iceberg en las capas Plata y Oro:

```bash
docker exec -it airflow-scheduler python /opt/airflow/jobs/spark/init_processed_tables.py
```

```bash
docker exec -it airflow-scheduler python /opt/airflow/jobs/spark/init_gold_tables.py
```

### Paso 2: Orquestación en Apache Airflow
1. Abre tu navegador e ingresa a la interfaz de Airflow: `http://localhost:8085`.
2. El usuario y contraseña por defecto es `admin` / `admin`.
3. Activa y ejecuta los DAGs manualmente en este orden:
   * **`dag_nba_ingesta`**: Mueve los datos analíticos y salarios hacia la Capa Bronce.
   * **`dag_nba_merge`**: Ejecuta los procesos de Spark para limpiar los datos y aplicar el *Fuzzy Matching* hacia la Capa Plata.
   * **`dag_nba_oro`**: Conecta con Trino para realizar los cálculos analíticos finales en la Capa Oro.

---

## 4. Configuración de Visualización (Metabase)

Dado que Metabase arranca como un lienzo en blanco, sigue estos pasos para conectar los datos y construir los gráficos:

### 4.1. Conexión de la Base de Datos
1. Accede a `http://localhost:3000` y completa la configuración inicial.
2. Añade una nueva base de datos con los siguientes parámetros de conexión a **Trino**:
   * **Tipo de base de datos:** Presto / Trino
   * **Host:** `trino`
   * **Puerto:** `8080`
   * **Catálogo:** `iceberg`
   * **Esquema:** `warehouse` (o el esquema donde residan tus tablas de la capa Oro)
   * **Usuario:** `admin` (sin contraseña)

### 4.2. Creación de Dashboards
Una vez conectado, utiliza el editor SQL de Metabase para introducir las consultas de negocio desarrolladas en el proyecto y generar las visualizaciones:
* **Gráfico de Dispersión (ROI):** Cruza el salario de los jugadores con su rendimiento global para analizar la eficiencia económica.
* **Gráfico de Barras (Load Management):** Agrupa por la situación de descanso (`rest_situation`) y calcula la media de valoración (`avg_rating`) para exponer el impacto de la fatiga en escenarios *Back-to-back*.
