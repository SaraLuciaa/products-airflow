from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import ( 
    col, count, avg, min, max, stddev, percentile_approx, desc, explode, split, 
    year, month, to_date, size, dayofweek, weekofyear, datediff, lag, sum as ssum, )
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

import logging
import json
import os

logger = logging.getLogger(__name__)

DATASET_PATH = "/opt/airflow/dataset"
REPORTS_PATH = "/opt/airflow/reports"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 30),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# =============================================================================
# TAREA 1: Revisión de estructura y calidad de datos
# =============================================================================
def revisar_estructura_y_calidad_transacciones(ti):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, length, size, split, min, max
    from pyspark.sql.types import IntegerType
    from pyspark.sql.functions import to_date, avg, stddev

    logger.info("=" * 80)
    logger.info("TAREA 1 - REVISIÓN ESTRUCTURA Y CALIDAD DE TRANSACCIONES")
    logger.info("=" * 80)

    spark = (
        SparkSession.builder.appName("Transacciones_RevisionInicial")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

    resultados = {}
    try:
        trans_path = os.path.join(DATASET_PATH, "Transactions")
        trans_files = [f for f in os.listdir(trans_path) if f.endswith("_Tran.csv")]
        logger.info(f"Archivos de transacciones encontrados: {len(trans_files)}")

        if not trans_files:
            logger.warning("No se encontraron archivos de transacciones")
            ti.xcom_push(key="revision_inicial", value={})
            return

        file_paths = [os.path.join(trans_path, f) for f in trans_files]
        df = spark.read.csv(file_paths, header=False, inferSchema=False, sep="|")
        df = df.toDF("date", "store_id", "customer_id", "products")

        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        df = df.withColumn("store_id", col("store_id").cast(IntegerType()))
        df = df.withColumn("customer_id", col("customer_id").cast(IntegerType()))

        num_filas = df.count()
        num_columnas = len(df.columns)
        tipos_datos = {field.name: str(field.dataType) for field in df.schema.fields}

        nulos_por_columna = {
            "date": df.filter(col("date").isNull()).count(),
            "store_id": df.filter(col("store_id").isNull()).count(),
            "customer_id": df.filter(col("customer_id").isNull()).count(),
            "products": df.filter(
                (col("products").isNull())
                | (col("products") == "")
                | (length(col("products")) == 0)
            ).count(),
        }

        num_duplicados_exactos = num_filas - df.dropDuplicates().count()
        num_duplicados_cliente_fecha = (
            num_filas - df.dropDuplicates(["date", "customer_id"]).count()
        )

        # Productos por transacción
        df = df.withColumn("productos_array", split(col("products"), " "))
        df = df.withColumn("num_productos", size(col("productos_array")))
        stats_productos = df.select(
            avg("num_productos").alias("promedio"),
            stddev("num_productos").alias("desv_std"),
            min("num_productos").alias("minimo"),
            max("num_productos").alias("maximo"),
        ).collect()[0]

        rango_fechas = (
            df.select(min("date").alias("fecha_min"), max("date").alias("fecha_max"))
            .collect()[0]
        )
        tiendas_unicas = df.select("store_id").distinct().count()

        resultados = {
            "archivos_procesados": trans_files,
            "num_archivos": len(trans_files),
            "tiendas_unicas": int(tiendas_unicas),
            "estructura": {
                "num_transacciones": int(num_filas),
                "num_columnas": int(num_columnas),
                "columnas": df.columns[:4],
                "tipos_datos": tipos_datos,
            },
            "calidad_datos": {
                "valores_nulos": {k: int(v) for k, v in nulos_por_columna.items()},
                "total_nulos": int(sum(nulos_por_columna.values())),
                "duplicados_exactos": int(num_duplicados_exactos),
                "duplicados_cliente_fecha": int(num_duplicados_cliente_fecha),
                "porcentaje_duplicados_exactos": round(
                    num_duplicados_exactos / num_filas * 100, 2
                )
                if num_filas > 0
                else 0,
            },
            "analisis_productos": {
                "productos_por_transaccion": {
                    "promedio": round(stats_productos["promedio"], 2)
                    if stats_productos["promedio"]
                    else 0,
                    "minimo": int(stats_productos["minimo"]),
                    "maximo": int(stats_productos["maximo"]),
                    "desviacion_estandar": round(
                        stats_productos["desv_std"], 2
                    )
                    if stats_productos["desv_std"]
                    else 0,
                }
            },
            "rango_temporal": {
                "fecha_inicio": str(rango_fechas["fecha_min"]),
                "fecha_fin": str(rango_fechas["fecha_max"]),
            },
        }

        ti.xcom_push(key="revision_inicial", value=resultados)
    except Exception as e:
        logger.error(f"Error en revisión inicial: {e}")
        raise
    finally:
        spark.stop()


# =============================================================================
# TAREA 2: Estadísticas descriptivas, distribuciones y top categorías
# =============================================================================
def calcular_estadisticas_ventas_y_categorias(ti):
    logger.info("=" * 80)
    logger.info("TAREA 2 - ESTADÍSTICAS, DISTRIBUCIONES Y TOP CATEGORÍAS")
    logger.info("=" * 80)

    spark = (
        SparkSession.builder.appName("Transacciones_Estadisticas_EDA")
        .master("local[*]")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    estadisticas = {}
    try:
        # ------------------ Leer transacciones ------------------
        trans_path = os.path.join(DATASET_PATH, "Transactions")
        trans_files = [f for f in os.listdir(trans_path) if f.endswith("_Tran.csv")]
        if not trans_files:
            logger.warning("No se encontraron archivos de transacciones")
            ti.xcom_push(key="estadisticas", value={})
            return

        file_paths = [os.path.join(trans_path, f) for f in trans_files]
        df = spark.read.csv(file_paths, header=False, inferSchema=False, sep="|").toDF(
            "date", "store_id", "customer_id", "products"
        )

        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        df = df.withColumn("store_id", col("store_id").cast(IntegerType()))
        df = df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
        df = df.withColumn("productos_array", split(col("products"), " "))
        df = df.withColumn("num_productos", size(col("productos_array")))

        total_transacciones = df.count()

        # ------------------ Estadísticas numéricas básicas ------------------
        stats_num_prod = df.select(
            avg("num_productos").alias("media"),
            stddev("num_productos").alias("desv_std"),
            min("num_productos").alias("minimo"),
            max("num_productos").alias("maximo"),
        ).collect()[0]

        p25_np, mediana_np, p75_np = (
            df.select(
                percentile_approx("num_productos", [0.25, 0.5, 0.75], 10000).alias("p")
            )
            .collect()[0]["p"]
        )

        moda_num_prod = (
            df.groupBy("num_productos").count().orderBy(desc("count")).first()
        )
        iqr_np = p75_np - p25_np
        outliers_num_prod = df.filter(
            (col("num_productos") < p25_np - 1.5 * iqr_np)
            | (col("num_productos") > p75_np + 1.5 * iqr_np)
        ).count()

        estadisticas_numericas = {
            "num_productos_por_transaccion": {
                "media": round(stats_num_prod["media"], 2)
                if stats_num_prod["media"]
                else 0,
                "mediana": int(mediana_np) if mediana_np else 0,
                "moda": moda_num_prod["num_productos"] if moda_num_prod else None,
                "desviacion_estandar": round(stats_num_prod["desv_std"], 2)
                if stats_num_prod["desv_std"]
                else 0,
                "minimo": int(stats_num_prod["minimo"]),
                "maximo": int(stats_num_prod["maximo"]),
                "percentil_25": int(p25_np) if p25_np else 0,
                "percentil_75": int(p75_np) if p75_np else 0,
                "rango_intercuartil_iqr": round(iqr_np, 2) if iqr_np else 0,
                "outliers_count": int(outliers_num_prod),
            }
        }

        # ------------------ Estadísticas categóricas (clientes y productos) ------------------
        df.cache()
        clientes_unicos = df.select("customer_id").distinct().count()
        moda_customer = (
            df.groupBy("customer_id").count().orderBy(desc("count")).first()
        )
        estadisticas_categoricas = {
            "customer_id": {
                "clientes_unicos": int(clientes_unicos),
                "moda": int(moda_customer["customer_id"]) if moda_customer else None,
            }
        }

        # Top 10 clientes por número de compras
        top_clientes_rows = (
            df.groupBy("customer_id")
            .count()
            .withColumnRenamed("count", "frecuencia")
            .orderBy(desc("frecuencia"))
            .limit(10)
            .collect()
        )
        clientes_data = []
        for row in top_clientes_rows:
            freq = row["frecuencia"]
            clientes_data.append(
                {
                    "customer_id": int(row["customer_id"]),
                    "frecuencia_absoluta": int(freq),
                    "frecuencia_relativa_pct": round(
                        (freq / total_transacciones) * 100, 2
                    )
                    if total_transacciones > 0
                    else 0,
                }
            )

        estadisticas_categoricas["top_10_clientes_mas_compras"] = {
            "total_clientes_unicos": int(clientes_unicos),
            "top_10": clientes_data,
        }

        # Top 10 productos
        df_productos_exploded = df.select(
            explode(split(col("products"), " ")).alias("product_id")
        )
        df_productos_exploded = df_productos_exploded.withColumn(
            "product_id", col("product_id").cast(IntegerType())
        ).filter(col("product_id").isNotNull())
        df_productos_exploded.cache()

        total_productos_vendidos = df_productos_exploded.count()
        top_productos_rows = (
            df_productos_exploded.groupBy("product_id")
            .count()
            .withColumnRenamed("count", "frecuencia")
            .orderBy(desc("frecuencia"))
            .limit(10)
            .collect()
        )
        productos_data = []
        for row in top_productos_rows:
            frecuencia = row["frecuencia"]
            productos_data.append(
                {
                    "product_id": int(row["product_id"]),
                    "frecuencia_absoluta": int(frecuencia),
                    "frecuencia_relativa_pct": round(
                        (frecuencia / total_productos_vendidos) * 100, 2
                    )
                    if total_productos_vendidos > 0
                    else 0,
                }
            )

        estadisticas_categoricas["top_10_productos_mas_vendidos"] = {
            "total_productos_vendidos": int(total_productos_vendidos),
            "productos_unicos": int(
                df_productos_exploded.select("product_id").distinct().count()
            ),
            "top_10": productos_data,
        }

        # ------------------ Distribuciones temporales ------------------
        from pyspark.sql.functions import dayofweek

        df = df.withColumn("year", year(col("date"))).withColumn(
            "month", month(col("date"))
        )

        # Mensual
        dist_mensual_rows = (
            df.groupBy("year", "month")
            .agg(count("*").alias("num_transacciones"))
            .orderBy("year", "month")
            .collect()
        )
        mensual_data = []
        for r in dist_mensual_rows:
            mensual_data.append(
                {
                    "year": int(r["year"]),
                    "month": int(r["month"]),
                    "num_transacciones": int(r["num_transacciones"]),
                    "porcentaje": round(
                        (r["num_transacciones"] / total_transacciones) * 100, 2
                    )
                    if total_transacciones > 0
                    else 0,
                }
            )
        estadisticas_categoricas["distribucion_temporal_mensual"] = mensual_data

        # Día de la semana
        df = df.withColumn("dia_semana", dayofweek(col("date")))
        dist_semanal_rows = (
            df.groupBy("dia_semana")
            .agg(count("*").alias("num_transacciones"))
            .orderBy("dia_semana")
            .collect()
        )
        dias_semana = {
            1: "Domingo",
            2: "Lunes",
            3: "Martes",
            4: "Miércoles",
            5: "Jueves",
            6: "Viernes",
            7: "Sábado",
        }
        semanal_data = []
        for r in dist_semanal_rows:
            semanal_data.append(
                {
                    "dia_semana": dias_semana.get(r["dia_semana"], "Desconocido"),
                    "num_transacciones": int(r["num_transacciones"]),
                    "porcentaje": round(
                        (r["num_transacciones"] / total_transacciones) * 100, 2
                    )
                    if total_transacciones > 0
                    else 0,
                }
            )
        estadisticas_categoricas["distribucion_dia_semana"] = semanal_data

        # Por tienda
        dist_tiendas_rows = (
            df.groupBy("store_id")
            .agg(count("*").alias("num_transacciones"))
            .orderBy(desc("num_transacciones"))
            .collect()
        )
        tiendas_data = []
        for r in dist_tiendas_rows:
            tiendas_data.append(
                {
                    "store_id": int(r["store_id"]),
                    "num_transacciones": int(r["num_transacciones"]),
                    "porcentaje": round(
                        (r["num_transacciones"] / total_transacciones) * 100, 2
                    )
                    if total_transacciones > 0
                    else 0,
                }
            )
        estadisticas_categoricas["distribucion_por_tienda"] = tiendas_data

        # Serie temporal diaria
        dist_diaria_rows = (
            df.groupBy("date")
            .agg(count("*").alias("num_transacciones"))
            .orderBy("date")
            .collect()
        )
        diaria_data = []
        for r in dist_diaria_rows:
            diaria_data.append(
                {
                    "date": str(r["date"]),
                    "num_transacciones": int(r["num_transacciones"]),
                }
            )
        estadisticas_categoricas["distribucion_temporal_diaria"] = diaria_data

        # Serie temporal semanal (año-semana)
        df = df.withColumn("week_of_year", weekofyear(col("date")))
        dist_semanal_cal_rows = (
            df.groupBy("year", "week_of_year")
            .agg(count("*").alias("num_transacciones"))
            .orderBy("year", "week_of_year")
            .collect()
        )
        semanal_cal_data = []
        for r in dist_semanal_cal_rows:
            semanal_cal_data.append(
                {
                    "year": int(r["year"]),
                    "week_of_year": int(r["week_of_year"]),
                    "num_transacciones": int(r["num_transacciones"]),
                }
            )
        estadisticas_categoricas["distribucion_temporal_semanal"] = semanal_cal_data

        # ------------------ Tiempo entre compras (global) ------------------
        w = Window.partitionBy("customer_id").orderBy(col("date").asc())
        df_tiempos = df.withColumn("prev_date", lag("date").over(w))
        df_tiempos = df_tiempos.withColumn(
            "diff_dias", datediff(col("date"), col("prev_date"))
        )
        tiempo_promedio_global = (
            df_tiempos.groupBy("customer_id")
            .agg(avg("diff_dias").alias("promedio_dias"))
            .select(avg("promedio_dias"))
            .collect()[0][0]
        )

        estadisticas_categoricas["tiempo_entre_compras"] = {
            "promedio_global_dias": float(round(tiempo_promedio_global, 2))
            if tiempo_promedio_global is not None
            else None
        }

        # ------------------ Top categorías por unidades vendidas ------------------
        try:
            prod_cat_path = os.path.join(
                DATASET_PATH, "Products", "ProductCategory.csv"
            )
            categories_path = os.path.join(
                DATASET_PATH, "Products", "Categories.csv"
            )

            df_prod_cat = (
                spark.read.csv(prod_cat_path, header=True, sep="|")
                .toDF("product_id_raw", "category_id")
            )
            df_prod_cat = (
                df_prod_cat.withColumn(
                    "product_id", col("product_id_raw").cast(IntegerType())
                )
                .withColumn("category_id", col("category_id").cast(IntegerType()))
                .select("product_id", "category_id")
            )

            df_cats = (
                spark.read.csv(categories_path, header=True, sep="|")
                .toDF("category_id", "category_name")
                .withColumn("category_id", col("category_id").cast(IntegerType()))
            )

            prod_counts = (
                df_productos_exploded.groupBy("product_id")
                .count()
                .withColumnRenamed("count", "units_sold")
            )

            prod_cat_join = prod_counts.join(
                df_prod_cat, on="product_id", how="left"
            )
            cat_counts = prod_cat_join.groupBy("category_id").agg(
                ssum("units_sold").alias("units_sold")
            )

            cat_with_names = cat_counts.join(
                df_cats, on="category_id", how="left"
            ).orderBy(col("units_sold").desc())

            top_n = 10
            top_rows = cat_with_names.limit(top_n).collect()
            total_units_row = cat_with_names.select(
                ssum("units_sold").alias("total")
            ).collect()[0]
            total_units = total_units_row["total"] if total_units_row["total"] else 0

            result_list = []
            for r in top_rows:
                units = int(r["units_sold"]) if r["units_sold"] is not None else 0
                cat_id = (
                    int(r["category_id"]) if r["category_id"] is not None else None
                )
                name = (
                    r["category_name"] if r["category_name"] is not None else None
                )
                pct = (
                    round((units / total_units) * 100, 2)
                    if total_units and units
                    else 0.0
                )
                result_list.append(
                    {
                        "category_id": cat_id,
                        "category_name": name,
                        "unidades_vendidas": units,
                        "porcentaje": pct,
                    }
                )

            top_categorias = {
                "top_n": top_n,
                "total_unidades": int(total_units) if total_units is not None else 0,
                "top": result_list,
            }
        except Exception as e:
            logger.warning(
                f"No se pudo calcular top categorías por ventas: {e}"
            )
            top_categorias = {
                "top_n": 0,
                "total_unidades": 0,
                "top": [],
            }

        # ------------------ Construir objeto de salida ------------------
        estadisticas = {
            "archivos_procesados": trans_files,
            "num_archivos": len(trans_files),
            "total_transacciones": int(total_transacciones),
            "estadisticas_numericas": estadisticas_numericas,
            "estadisticas_categoricas": estadisticas_categoricas,
            "top_categorias_por_ventas": top_categorias,
        }

        df.unpersist()
        df_productos_exploded.unpersist()

        ti.xcom_push(key="estadisticas", value=estadisticas)
    except Exception as e:
        logger.error(f"Error en estadísticas: {e}")
        raise
    finally:
        spark.stop()


# =============================================================================
# TAREA 3: Generar JSON final de EDA (un solo archivo)
# =============================================================================
def generar_json_eda_transacciones(ti):
    logger.info("=" * 80)
    logger.info("TAREA 3 - GENERAR JSON EDA TRANSACCIONES")
    logger.info("=" * 80)

    try:
        revision = ti.xcom_pull(
            key="revision_inicial",
            task_ids="revisar_estructura_y_calidad_transacciones",
        )
        estadisticas = ti.xcom_pull(
            key="estadisticas",
            task_ids="calcular_estadisticas_ventas_y_categorias",
        )

        if not revision:
            logger.warning("No se encontraron datos de revisión inicial")
            revision = {}

        if not estadisticas:
            logger.warning("No se encontraron estadísticas de ventas")
            estadisticas = {}

        eda = {
            "revision_inicial": revision,
            "estadisticas": estadisticas,
        }

        os.makedirs(REPORTS_PATH, exist_ok=True)
        output_path = os.path.join(REPORTS_PATH, "transacciones_eda.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(eda, f, indent=4, ensure_ascii=False)

        logger.info(f"JSON de EDA generado en: {output_path}")
    except Exception as e:
        logger.error(f"Error generando JSON EDA: {e}")
        raise

# =============================================================================
# DEFINICIÓN DEL DAG
# =============================================================================
with DAG(
    "transacciones_eda_basica",
    default_args=default_args,
    description="EDA básico de archivos de transacciones con estadísticas y distribuciones para dashboard",
    schedule_interval=None,
    catchup=False,
    tags=["transacciones", "eda", "dashboard"],
) as dag:

    tarea_revision = PythonOperator(
        task_id="revisar_estructura_y_calidad_transacciones",
        python_callable=revisar_estructura_y_calidad_transacciones,
    )

    tarea_estadisticas = PythonOperator(
        task_id="calcular_estadisticas_ventas_y_categorias",
        python_callable=calcular_estadisticas_ventas_y_categorias,
    )

    tarea_json = PythonOperator(
        task_id="generar_json_eda_transacciones",
        python_callable=generar_json_eda_transacciones,
    )

    tarea_revision >> tarea_estadisticas >> tarea_json