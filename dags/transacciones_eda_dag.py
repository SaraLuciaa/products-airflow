from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import json
import os

logger = logging.getLogger(__name__)

DATASET_PATH = "/opt/airflow/dataset"
REPORTS_PATH = "/opt/airflow/reports"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ================================================================================
# TAREA 1: Revisión Inicial (igual que tu versión)
# ================================================================================
def revision_inicial_transacciones(ti):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, length, size, split, min, max
    from pyspark.sql.types import IntegerType
    from pyspark.sql.functions import to_date, avg, stddev, year, month

    logger.info("=" * 80)
    logger.info("REVISIÓN INICIAL - ARCHIVOS DE TRANSACCIONES")
    logger.info("=" * 80)

    spark = SparkSession.builder \
        .appName("Transacciones_RevisionInicial") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

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

        # Nulos (rápido)
        nulos_por_columna = {
            "date": df.filter(col("date").isNull()).count(),
            "store_id": df.filter(col("store_id").isNull()).count(),
            "customer_id": df.filter(col("customer_id").isNull()).count(),
            "products": df.filter((col("products").isNull()) | (col("products") == "") | (length(col("products")) == 0)).count()
        }

        num_duplicados_exactos = num_filas - df.dropDuplicates().count()
        num_duplicados_cliente_fecha = num_filas - df.dropDuplicates(["date", "customer_id"]).count()

        # Productos por transacción
        df = df.withColumn("productos_array", split(col("products"), " "))
        df = df.withColumn("num_productos", size(col("productos_array")))
        stats_productos = df.select(
            avg("num_productos").alias("promedio"),
            stddev("num_productos").alias("desv_std"),
            min("num_productos").alias("minimo"),
            max("num_productos").alias("maximo")
        ).collect()[0]

        df = df.withColumn("year", year(col("date"))).withColumn("month", month(col("date")))
        rango_fechas = df.select(min("date").alias("fecha_min"), max("date").alias("fecha_max")).collect()[0]
        tiendas_unicas = df.select("store_id").distinct().count()

        resultados["global"] = {
            "archivos_procesados": trans_files,
            "num_archivos": len(trans_files),
            "tiendas_unicas": tiendas_unicas,
            "estructura": {
                "num_transacciones": num_filas,
                "num_columnas": num_columnas,
                "columnas": df.columns[:4],
                "tipos_datos": tipos_datos
            },
            "calidad_datos": {
                "valores_nulos": nulos_por_columna,
                "total_nulos": sum(nulos_por_columna.values()),
                "duplicados_exactos": num_duplicados_exactos,
                "duplicados_cliente_fecha": num_duplicados_cliente_fecha,
                "porcentaje_duplicados_exactos": round(num_duplicados_exactos / num_filas * 100, 2) if num_filas > 0 else 0,
            },
            "analisis_productos": {
                "productos_por_transaccion": {
                    "promedio": round(stats_productos["promedio"], 2) if stats_productos["promedio"] else 0,
                    "minimo": stats_productos["minimo"],
                    "maximo": stats_productos["maximo"],
                    "desviacion_estandar": round(stats_productos["desv_std"], 2) if stats_productos["desv_std"] else 0
                }
            },
            "rango_temporal": {
                "fecha_inicio": str(rango_fechas["fecha_min"]),
                "fecha_fin": str(rango_fechas["fecha_max"])
            }
        }

        os.makedirs(REPORTS_PATH, exist_ok=True)
        output_path = os.path.join(REPORTS_PATH, "transacciones_revision_inicial.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(resultados, f, indent=4, ensure_ascii=False)
        ti.xcom_push(key="revision_inicial", value=resultados)
    except Exception as e:
        logger.error(f"Error en revisión inicial: {e}")
        raise
    finally:
        spark.stop()

# ================================================================================
# TAREA 2: Estadísticas descriptivas (tu versión extendida previa)
# ================================================================================
def estadisticas_descriptivas_transacciones(ti):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, count, avg, min, max, stddev, percentile_approx, desc, explode, split,
        year, month, dayofweek, to_date, size, lit, when, datediff, hour
    )
    from pyspark.sql.types import IntegerType, TimestampType
    from pyspark.sql.window import Window

    logger.info("=" * 80)
    logger.info("ESTADÍSTICAS DESCRIPTIVAS - TRANSACCIONES (EXTENDIDO)")
    logger.info("=" * 80)

    spark = SparkSession.builder \
        .appName("Transacciones_Estadisticas_Ext") \
        .master("local[2]") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    estadisticas = {}
    try:
        trans_path = os.path.join(DATASET_PATH, "Transactions")
        trans_files = [f for f in os.listdir(trans_path) if f.endswith("_Tran.csv")]
        if not trans_files:
            logger.warning("No se encontraron archivos de transacciones")
            ti.xcom_push(key="estadisticas", value={})
            return

        file_paths = [os.path.join(trans_path, f) for f in trans_files]
        df = spark.read.csv(file_paths, header=False, inferSchema=False, sep="|").toDF("date", "store_id", "customer_id", "products")

        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        df = df.withColumn("store_id", col("store_id").cast(IntegerType()))
        df = df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
        df = df.withColumn("productos_array", split(col("products"), " "))
        df = df.withColumn("num_productos", size(col("productos_array")))

        total_transacciones = df.count()

        # Numéricas
        stats_num_prod = df.select(
            avg("num_productos").alias("media"),
            stddev("num_productos").alias("desv_std"),
            min("num_productos").alias("minimo"),
            max("num_productos").alias("maximo")
        ).collect()[0]
        p25_np, mediana_np, p75_np = df.select(
            percentile_approx("num_productos", [0.25, 0.5, 0.75], 10000).alias("p")
        ).collect()[0]["p"]
        moda_num_prod = df.groupBy("num_productos").count().orderBy(desc("count")).first()
        iqr_np = p75_np - p25_np
        outliers_num_prod = df.filter((col("num_productos") < p25_np - 1.5 * iqr_np) | (col("num_productos") > p75_np + 1.5 * iqr_np)).count()

        estadisticas_numericas = {
            "num_productos_por_transaccion": {
                "media": round(stats_num_prod["media"], 2) if stats_num_prod["media"] else 0,
                "mediana": int(mediana_np) if mediana_np else 0,
                "moda": moda_num_prod["num_productos"] if moda_num_prod else None,
                "desviacion_estandar": round(stats_num_prod["desv_std"], 2) if stats_num_prod["desv_std"] else 0,
                "minimo": stats_num_prod["minimo"],
                "maximo": stats_num_prod["maximo"],
                "percentil_25": int(p25_np) if p25_np else 0,
                "percentil_75": int(p75_np) if p75_np else 0,
                "rango_intercuartil_iqr": round(iqr_np, 2) if iqr_np else 0,
                "outliers_count": outliers_num_prod
            }
        }

        # Categóricas
        df.cache()
        clientes_unicos = df.select("customer_id").distinct().count()
        moda_customer = df.groupBy("customer_id").count().orderBy(desc("count")).first()
        estadisticas_categoricas = {"customer_id": {"clientes_unicos": clientes_unicos, "moda": moda_customer["customer_id"] if moda_customer else None}}

        df_productos_exploded = df.select(explode(split(col("products"), " ")).alias("product_id"))
        df_productos_exploded = df_productos_exploded.withColumn("product_id", col("product_id").cast(IntegerType())).filter(col("product_id").isNotNull())
        df_productos_exploded.cache()
        total_productos_vendidos = df_productos_exploded.count()
        top_productos = df_productos_exploded.groupBy("product_id").count().withColumnRenamed("count", "frecuencia").orderBy(desc("frecuencia")).limit(30).collect()
        productos_data = []
        for row in top_productos:
            frecuencia = row["frecuencia"]
            productos_data.append({
                "product_id": row["product_id"],
                "frecuencia_absoluta": frecuencia,
                "frecuencia_relativa_pct": round((frecuencia / total_productos_vendidos) * 100, 2) if total_productos_vendidos > 0 else 0
            })
        estadisticas_categoricas["top_30_productos_mas_vendidos"] = {
            "total_productos_vendidos": total_productos_vendidos,
            "productos_unicos": df_productos_exploded.select("product_id").distinct().count(),
            "top_30": productos_data
        }

        from pyspark.sql.functions import dayofweek
        df = df.withColumn("year", year(col("date"))).withColumn("month", month(col("date"))).withColumn("dia_semana", dayofweek(col("date")))
        dist_mensual = df.groupBy("year", "month").agg(count("*").alias("num_transacciones")).orderBy("year", "month").collect()
        mensual_data = [{"year": r["year"], "month": r["month"], "num_transacciones": r["num_transacciones"], "porcentaje": round((r["num_transacciones"]/total_transacciones)*100, 2) if total_transacciones>0 else 0} for r in dist_mensual]
        estadisticas_categoricas["distribucion_temporal_mensual"] = mensual_data

        dist_semanal = df.groupBy("dia_semana").agg(count("*").alias("num_transacciones")).orderBy("dia_semana").collect()
        dias_semana = {1: "Domingo", 2: "Lunes", 3: "Martes", 4: "Miércoles", 5: "Jueves", 6: "Viernes", 7: "Sábado"}
        semanal_data = [{"dia_semana": dias_semana.get(r["dia_semana"], "Desconocido"), "num_transacciones": r["num_transacciones"], "porcentaje": round((r["num_transacciones"]/total_transacciones)*100, 2) if total_transacciones>0 else 0} for r in dist_semanal]
        estadisticas_categoricas["distribucion_dia_semana"] = semanal_data

        dist_tiendas = df.groupBy("store_id").agg(count("*").alias("num_transacciones")).orderBy(desc("num_transacciones")).collect()
        tiendas_data = [{"store_id": r["store_id"], "num_transacciones": r["num_transacciones"], "porcentaje": round((r["num_transacciones"]/total_transacciones)*100, 2) if total_transacciones>0 else 0} for r in dist_tiendas]
        estadisticas_categoricas["distribucion_por_tienda"] = tiendas_data

        # Tiempo entre compras (global)
        from pyspark.sql.window import Window
        from pyspark.sql.functions import lag, datediff
        w = Window.partitionBy("customer_id").orderBy(col("date").asc())
        df_tiempos = df.withColumn("prev_date", lag("date").over(w))
        df_tiempos = df_tiempos.withColumn("diff_dias", datediff(col("date"), col("prev_date")))
        tiempo_promedio_global = df_tiempos.groupBy("customer_id").agg(avg("diff_dias").alias("promedio_dias")).select(avg("promedio_dias")).collect()[0][0]
        estadisticas_categoricas["tiempo_entre_compras"] = {"promedio_global_dias": float(round(tiempo_promedio_global, 2)) if tiempo_promedio_global is not None else None}

        # Reglas de asociación (FPGrowth)
        from pyspark.ml.fpm import FPGrowth
        fpGrowth = FPGrowth(itemsCol="productos_array", minSupport=0.02, minConfidence=0.3)
        model = fpGrowth.fit(df)
        reglas_df = model.associationRules.select("antecedent", "consequent", "confidence", "lift", "support").orderBy(desc("lift"))
        reglas = []
        for row in reglas_df.limit(100).collect():
            reglas.append({
                "antecedente": [str(x) for x in row["antecedent"]],
                "consecuente": [str(x) for x in row["consequent"]],
                "confidence": float(round(row["confidence"], 4)),
                "lift": float(round(row["lift"], 4)),
                "support": float(round(row["support"], 4)),
            })

        estadisticas["global"] = {
            "archivos_procesados": trans_files,
            "num_archivos": len(trans_files),
            "total_transacciones": total_transacciones,
            "estadisticas_numericas": estadisticas_numericas,
            "estadisticas_categoricas": {
                **estadisticas_categoricas,
                "reglas_asociacion_top": reglas
            }
        }

        df.unpersist()
        df_productos_exploded.unpersist()

        os.makedirs(REPORTS_PATH, exist_ok=True)
        output_path = os.path.join(REPORTS_PATH, "transacciones_estadisticas_descriptivas.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(estadisticas, f, indent=4, ensure_ascii=False)
        ti.xcom_push(key="estadisticas", value=estadisticas)
    except Exception as e:
        logger.error(f"Error en estadísticas: {e}")
        raise
    finally:
        spark.stop()

# ================================================================================
# TAREA 4 (NUEVA): Cohortes y Retención
# ================================================================================
def cohortes_retencion(ti):
    """
    Construye:
    - Tabla de tamaños de cohorte (mes de primera compra)
    - Matriz de retención (% clientes activos por cohorte y mes desde la primera compra)
    Guarda JSON y devuelve en XCom para el consolidado.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, to_date, trunc, min as smin, countDistinct, count, months_between, floor
    from pyspark.sql.types import IntegerType

    logger.info("=" * 80)
    logger.info("COHORTES Y RETENCIÓN")
    logger.info("=" * 80)

    spark = SparkSession.builder \
        .appName("Transacciones_Cohortes") \
        .master("local[*]") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    try:
        trans_path = os.path.join(DATASET_PATH, "Transactions")
        trans_files = [f for f in os.listdir(trans_path) if f.endswith("_Tran.csv")]
        if not trans_files:
            ti.xcom_push(key="cohortes", value={})
            return

        file_paths = [os.path.join(trans_path, f) for f in trans_files]
        df = spark.read.csv(file_paths, header=False, inferSchema=False, sep="|").toDF("date", "store_id", "customer_id", "products")

        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        df = df.withColumn("store_id", col("store_id").cast(IntegerType()))
        df = df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
        df = df.withColumn("cohort_month", trunc(col("date"), "MM"))

        # primera compra por cliente
        first_purchase = df.groupBy("customer_id").agg(smin("cohort_month").alias("first_month"))
        df = df.join(first_purchase, on="customer_id", how="left")
        # índice de cohorte (meses desde first_month)
        df = df.withColumn("cohort_index", floor(months_between(col("cohort_month"), col("first_month"))).cast("int"))

        # tamaño por cohorte (clientes únicos en mes 0)
        cohort_sizes = df.filter(col("cohort_index") == 0) \
                         .groupBy("first_month") \
                         .agg(countDistinct("customer_id").alias("cohort_size"))

        # clientes activos por cohorte e índice
        active_by_index = df.groupBy("first_month", "cohort_index") \
                            .agg(countDistinct("customer_id").alias("active_customers"))

        # Retención (= activos / tamaño_cohorte)
        joined = active_by_index.join(cohort_sizes, on="first_month", how="left")
        retention = joined.withColumn("retention_rate", (col("active_customers") / col("cohort_size")))

        # Colectar a listas para JSON
        cohort_sizes_rows = cohort_sizes.orderBy("first_month").collect()
        cohort_sizes_list = [{"cohort_month": str(r["first_month"]), "cohort_size": int(r["cohort_size"])} for r in cohort_sizes_rows]

        retention_rows = retention.orderBy("first_month", "cohort_index").collect()
        retention_list = [{
            "cohort_month": str(r["first_month"]),
            "cohort_index": int(r["cohort_index"]),
            "active_customers": int(r["active_customers"]),
            "cohort_size": int(r["cohort_size"]) if r["cohort_size"] is not None else None,
            "retention_rate": float(round(r["retention_rate"], 4)) if r["retention_rate"] is not None else None
        } for r in retention_rows]

        result = {
            "cohort_sizes": cohort_sizes_list,
            "retention": retention_list
        }

        os.makedirs(REPORTS_PATH, exist_ok=True)
        output_path = os.path.join(REPORTS_PATH, "transacciones_cohortes_retencion.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4, ensure_ascii=False)

        ti.xcom_push(key="cohortes", value=result)
    except Exception as e:
        logger.error(f"Error en cohortes/retención: {e}")
        raise
    finally:
        spark.stop()

# ================================================================================
# TAREA 5 (NUEVA): Forecast mensual por tienda y global
# ================================================================================
def forecasting_transacciones(ti, periods_ahead=6):
    """
    Pronóstico mensual de transacciones (global y por tienda).
    Intenta usar ExponentialSmoothing; si falla, usa promedio estacional naive.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, to_date, trunc, count as scount
    from pyspark.sql.types import IntegerType

    logger.info("=" * 80)
    logger.info("FORECAST MENSUAL - GLOBAL Y POR TIENDA")
    logger.info("=" * 80)

    spark = SparkSession.builder \
        .appName("Transacciones_Forecast") \
        .master("local[*]") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    try:
        trans_path = os.path.join(DATASET_PATH, "Transactions")
        trans_files = [f for f in os.listdir(trans_path) if f.endswith("_Tran.csv")]
        if not trans_files:
            ti.xcom_push(key="forecast", value={})
            return

        file_paths = [os.path.join(trans_path, f) for f in trans_files]
        df = spark.read.csv(file_paths, header=False, inferSchema=False, sep="|").toDF("date", "store_id", "customer_id", "products")

        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        df = df.withColumn("store_id", col("store_id").cast(IntegerType()))
        df = df.withColumn("month", trunc(col("date"), "MM"))

        # Agregados mensuales
        monthly_global = df.groupBy("month").agg(scount("*").alias("transactions")).orderBy("month")
        monthly_store = df.groupBy("store_id", "month").agg(scount("*").alias("transactions")).orderBy("store_id", "month")

        # Helper de forecast en pandas
        def _forecast_series_pdf(pdf, is_global=False):
            """
            Espera columnas: ['month', 'transactions'] o ['store_id','month','transactions']
            Retorna dataframe con columnas: ['store_id'(opcional),'month','yhat','method']
            """
            import pandas as pd
            pdf = pdf.sort_values("month")
            idx_col = None
            if "store_id" in pdf.columns:
                idx_col = int(pdf["store_id"].iloc[0])

            ts = pdf.set_index("month")["transactions"].astype(float)
            # Relleno de meses faltantes para estacionalidad estable
            ts = ts.asfreq("MS").fillna(0.0)

            yhat = None
            method = None
            try:
                # Statsmodels ES
                from statsmodels.tsa.holtwinters import ExponentialSmoothing
                seasonal = "add"
                trend = "add"
                seasonal_periods = 12
                if len(ts) >= max(18, seasonal_periods+6):
                    model = ExponentialSmoothing(ts, trend=trend, seasonal=seasonal, seasonal_periods=seasonal_periods, initialization_method="estimated")
                    fit = model.fit(optimized=True)
                    yhat = fit.forecast(periods_ahead)
                    method = "ExponentialSmoothing"
            except Exception as e:
                # fall back abajo
                method = None

            if yhat is None:
                # Naive estacional: promedio por mes del año
                grp = ts.groupby(ts.index.month).mean()
                future_idx = pd.date_range(start=ts.index[-1] + pd.offsets.MonthBegin(1), periods=periods_ahead, freq="MS")
                yhat_vals = []
                for d in future_idx:
                    mu = grp.get(d.month, ts.mean())
                    yhat_vals.append(mu if pd.notnull(mu) else ts.mean())
                yhat = pd.Series(yhat_vals, index=future_idx)
                method = "Naive-Seasonal-Avg"

            out = yhat.to_frame(name="yhat").reset_index().rename(columns={"index": "month"})
            if idx_col is not None:
                out.insert(0, "store_id", idx_col)
            out["method"] = method
            return out

        # Global
        pdf_global = monthly_global.toPandas()
        fcst_global = _forecast_series_pdf(pdf_global, is_global=True)

        # Por tienda
        # Para eficiencia, computamos en pandas por grupo (volumen moderado esperado)
        pdf_store = monthly_store.toPandas()
        fcst_stores_list = []
        for sid, g in pdf_store.groupby("store_id"):
            fcst_stores_list.append(_forecast_series_pdf(g))
        import pandas as pd
        fcst_store = pd.concat(fcst_stores_list, ignore_index=True) if fcst_stores_list else pd.DataFrame(columns=["store_id","month","yhat","method"])

        # Convertir a listas JSON-friendly
        forecast_global = [{"month": str(r["month"].date()), "yhat": float(r["yhat"]), "method": r["method"]} for _, r in fcst_global.iterrows()]
        forecast_tiendas = [{"store_id": int(r["store_id"]), "month": str(r["month"].date()), "yhat": float(r["yhat"]), "method": r["method"]} for _, r in fcst_store.iterrows()]

        result = {
            "horizon_months": periods_ahead,
            "global": forecast_global,
            "tiendas": forecast_tiendas
        }

        os.makedirs(REPORTS_PATH, exist_ok=True)
        output_path = os.path.join(REPORTS_PATH, "transacciones_forecast_mensual.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4, ensure_ascii=False)

        ti.xcom_push(key="forecast", value=result)
    except Exception as e:
        logger.error(f"Error en forecasting: {e}")
        raise
    finally:
        spark.stop()

# ================================================================================
# TAREA 3 (EXTENDIDA): Reporte consolidado con nuevas hojas
# ================================================================================
def generar_reporte_consolidado_transacciones(ti):
    import pandas as pd

    logger.info("=" * 80)
    logger.info("GENERANDO REPORTE CONSOLIDADO - TRANSACCIONES (EXTENDIDO)")
    logger.info("=" * 80)

    try:
        revision = ti.xcom_pull(key="revision_inicial", task_ids="revision_inicial_transacciones")
        estadisticas = ti.xcom_pull(key="estadisticas", task_ids="estadisticas_descriptivas_transacciones")
        cohortes = ti.xcom_pull(key="cohortes", task_ids="cohortes_retencion")
        forecast = ti.xcom_pull(key="forecast", task_ids="forecasting_transacciones")

        if not revision or not estadisticas:
            logger.warning("No se encontraron datos base de tareas anteriores")
            return

        rev_global = revision.get("global", {})
        est_global = estadisticas.get("global", {})

        resumen_data = [{
            "Alcance": "Global",
            "Archivos_Procesados": ", ".join(rev_global.get("archivos_procesados", [])),
            "Num_Archivos": rev_global.get("num_archivos", 0),
            "Tiendas_Unicas": rev_global.get("tiendas_unicas", 0),
            "Total_Transacciones": rev_global.get("estructura", {}).get("num_transacciones", 0),
            "Clientes_Unicos": est_global.get("estadisticas_categoricas", {}).get("customer_id", {}).get("clientes_unicos", 0),
            "Fecha_Inicio": rev_global.get("rango_temporal", {}).get("fecha_inicio", ""),
            "Fecha_Fin": rev_global.get("rango_temporal", {}).get("fecha_fin", ""),
            "Productos_Promedio_Transaccion": rev_global.get("analisis_productos", {}).get("productos_por_transaccion", {}).get("promedio", 0),
            "Duplicados": rev_global.get("calidad_datos", {}).get("duplicados_exactos", 0),
            "Nulos": rev_global.get("calidad_datos", {}).get("total_nulos", 0),
            "Productos_Unicos": est_global.get("estadisticas_categoricas", {}).get("top_30_productos_mas_vendidos", {}).get("productos_unicos", 0)
        }]

        os.makedirs(REPORTS_PATH, exist_ok=True)
        excel_path = os.path.join(REPORTS_PATH, "Transacciones_Reporte_Completo_Global.xlsx")

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            # Resumen
            pd.DataFrame(resumen_data).to_excel(writer, sheet_name="Resumen General", index=False)

            # Top productos
            if "estadisticas_categoricas" in est_global and "top_30_productos_mas_vendidos" in est_global["estadisticas_categoricas"]:
                df_productos = pd.DataFrame(est_global["estadisticas_categoricas"]["top_30_productos_mas_vendidos"]["top_30"])
                df_productos.to_excel(writer, sheet_name="Top_30_Productos", index=False)

            # Mensual
            if "estadisticas_categoricas" in est_global and "distribucion_temporal_mensual" in est_global["estadisticas_categoricas"]:
                df_mensual = pd.DataFrame(est_global["estadisticas_categoricas"]["distribucion_temporal_mensual"])
                df_mensual.to_excel(writer, sheet_name="Distribucion_Mensual", index=False)

            # Semanal
            if "estadisticas_categoricas" in est_global and "distribucion_dia_semana" in est_global["estadisticas_categoricas"]:
                df_semanal = pd.DataFrame(est_global["estadisticas_categoricas"]["distribucion_dia_semana"])
                df_semanal.to_excel(writer, sheet_name="Distribucion_Semanal", index=False)

            # Tiendas
            if "estadisticas_categoricas" in est_global and "distribucion_por_tienda" in est_global["estadisticas_categoricas"]:
                df_tiendas = pd.DataFrame(est_global["estadisticas_categoricas"]["distribucion_por_tienda"])
                df_tiendas.to_excel(writer, sheet_name="Distribucion_Tiendas", index=False)

            # Frecuencia clientes
            if "estadisticas_categoricas" in est_global and "frecuencia_compra_clientes" in est_global["estadisticas_categoricas"]:
                df_frecuencia = pd.DataFrame(est_global["estadisticas_categoricas"]["frecuencia_compra_clientes"]["top_20_frecuencias"])
                df_frecuencia.to_excel(writer, sheet_name="Frecuencia_Clientes", index=False)

            # Tiempo entre compras
            if "estadisticas_categoricas" in est_global and "tiempo_entre_compras" in est_global["estadisticas_categoricas"]:
                df_tiempo = pd.DataFrame([est_global["estadisticas_categoricas"]["tiempo_entre_compras"]])
                df_tiempo.to_excel(writer, sheet_name="Tiempo_Entre_Compras", index=False)

            # Cohortes (nuevas hojas)
            if cohortes:
                if "cohort_sizes" in cohortes:
                    pd.DataFrame(cohortes["cohort_sizes"]).to_excel(writer, sheet_name="Cohortes", index=False)

                # Matriz de retención pivot (cohort_month x cohort_index)
                if "retention" in cohortes and cohortes["retention"]:
                    df_ret = pd.DataFrame(cohortes["retention"])
                    if not df_ret.empty:
                        pivot = df_ret.pivot_table(index="cohort_month", columns="cohort_index", values="retention_rate", aggfunc="mean")
                        pivot = pivot.sort_index()
                        pivot.to_excel(writer, sheet_name="Retencion")

            # Forecast (nuevas hojas)
            if forecast:
                if "global" in forecast and forecast["global"]:
                    pd.DataFrame(forecast["global"]).to_excel(writer, sheet_name="Forecast_Global", index=False)
                if "tiendas" in forecast and forecast["tiendas"]:
                    pd.DataFrame(forecast["tiendas"]).to_excel(writer, sheet_name="Forecast_Tiendas", index=False)

        # CSV resumen
        csv_path = os.path.join(REPORTS_PATH, "Transacciones_Resumen_Global.csv")
        import pandas as pd
        pd.DataFrame(resumen_data).to_csv(csv_path, index=False, encoding="utf-8")

    except Exception as e:
        logger.error(f"Error generando reporte consolidado: {e}")
        raise

# ================================================================================
# DEFINICIÓN DEL DAG (actualizado con nuevas tareas)
# ================================================================================
with DAG(
    'transacciones_eda_completo',
    default_args=default_args,
    description='EDA completo de archivos de transacciones con análisis estadístico detallado',
    schedule_interval=None,
    catchup=False,
    tags=['transacciones', 'eda', 'retail']
) as dag:

    tarea_1 = PythonOperator(
        task_id='revision_inicial_transacciones',
        python_callable=revision_inicial_transacciones
    )

    tarea_2 = PythonOperator(
        task_id='estadisticas_descriptivas_transacciones',
        python_callable=estadisticas_descriptivas_transacciones
    )

    # NUEVAS
    tarea_4 = PythonOperator(
        task_id='cohortes_retencion',
        python_callable=cohortes_retencion
    )

    tarea_5 = PythonOperator(
        task_id='forecasting_transacciones',
        python_callable=forecasting_transacciones
    )

    tarea_3 = PythonOperator(
        task_id='generar_reporte_consolidado_transacciones',
        python_callable=generar_reporte_consolidado_transacciones
    )

    # Dependencias: primero EDA, luego cohortes + forecast, y al final el consolidado
    tarea_1 >> tarea_2
    tarea_2 >> [tarea_4, tarea_5]
    [tarea_4, tarea_5] >> tarea_3