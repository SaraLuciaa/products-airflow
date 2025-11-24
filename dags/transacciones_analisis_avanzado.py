from airflow import DAG
from airflow.operators.python import PythonOperator
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    split,
    size,
    explode,
    count,
    countDistinct,
    sum as ssum,
    trunc,
)
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.fpm import FPGrowth

import pandas as pd
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
# TAREA 1: Segmentación de clientes con K-Means
# =============================================================================
def segmentar_clientes_kmeans(ti, k=4):
    """
    Segmenta clientes usando K-Means a partir de:
      - frecuencia: nº de transacciones
      - volumen_total: total de productos comprados
      - diversidad_productos: nº de productos distintos
      - diversidad_categorias: nº de categorías distintas
    """

    logger.info("=" * 80)
    logger.info("TAREA 1 - SEGMENTACIÓN DE CLIENTES (K-MEANS)")
    logger.info("=" * 80)

    spark = (
        SparkSession.builder.appName("Transacciones_Segmentacion_Clientes")
        .master("local[*]")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .getOrCreate()
    )

    try:
        trans_path = os.path.join(DATASET_PATH, "Transactions")
        trans_files = [f for f in os.listdir(trans_path) if f.endswith("_Tran.csv")]
        if not trans_files:
            logger.warning("No se encontraron archivos de transacciones para segmentación")
            ti.xcom_push(key="segmentacion_clientes", value={})
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

        # Frecuencia y volumen total por cliente
        df_cliente = (
            df.groupBy("customer_id")
            .agg(
                count("*").alias("frecuencia"),
                ssum("num_productos").alias("volumen_total"),
            )
        )

        # Explode para obtener productos por cliente
        df_expl = df.select(
            "customer_id", explode(col("productos_array")).alias("product_raw")
        )
        df_expl = df_expl.withColumn(
            "product_id", col("product_raw").cast(IntegerType())
        ).filter(col("product_id").isNotNull())

        # Diversidad de productos
        df_div_prod = df_expl.groupBy("customer_id").agg(
            countDistinct("product_id").alias("diversidad_productos")
        )

        # Diversidad de categorías (product -> category)
        prod_cat_path = os.path.join(DATASET_PATH, "Products", "ProductCategory.csv")
        categories_path = os.path.join(DATASET_PATH, "Products", "Categories.csv")
        try:
            df_prod_cat = (
                spark.read.csv(prod_cat_path, header=True, sep="|")
                .toDF("product_id_raw", "category_id")
                .withColumn("product_id", col("product_id_raw").cast(IntegerType()))
                .withColumn("category_id", col("category_id").cast(IntegerType()))
                .select("product_id", "category_id")
            )

            df_expl_cat = df_expl.join(df_prod_cat, on="product_id", how="left")
            df_div_cat = df_expl_cat.groupBy("customer_id").agg(
                countDistinct("category_id").alias("diversidad_categorias")
            )
        except Exception as e:
            logger.warning(
                f"No se pudo cargar ProductCategory/Categories para diversidad_categorias: {e}"
            )
            df_div_cat = df_cliente.select("customer_id").withColumn(
                "diversidad_categorias", col("customer_id") * 0
            )  # cero categorías como fallback

        # Unir todas las features
        features_df = (
            df_cliente.join(df_div_prod, on="customer_id", how="left")
            .join(df_div_cat, on="customer_id", how="left")
            .fillna(0, subset=["diversidad_productos", "diversidad_categorias"])
        )

        # Filtrar clientes válidos
        features_df = features_df.filter(col("frecuencia") > 0)

        num_clientes = features_df.count()
        if num_clientes == 0:
            logger.warning("No hay clientes con datos suficientes para segmentación")
            ti.xcom_push(key="segmentacion_clientes", value={})
            return

        if num_clientes < k:
            logger.warning(
                f"Clientes ({num_clientes}) < k ({k}). Ajustando k a {num_clientes}."
            )
            k = num_clientes

        feature_cols = [
            "frecuencia",
            "volumen_total",
            "diversidad_productos",
            "diversidad_categorias",
        ]

        assembler = VectorAssembler(
            inputCols=feature_cols, outputCol="features", handleInvalid="keep"
        )
        assembled = assembler.transform(features_df)

        scaler = StandardScaler(
            inputCol="features",
            outputCol="features_scaled",
            withMean=True,
            withStd=True,
        )
        scaled = scaler.fit(assembled).transform(assembled)

        kmeans = KMeans(
            featuresCol="features_scaled",
            predictionCol="cluster",
            k=k,
            seed=42,
        )
        model = kmeans.fit(scaled)
        clustered = model.transform(scaled)

        # Resumen de clusters en escala original
        from pyspark.sql.functions import avg

        resumen_rows = (
            clustered.groupBy("cluster")
            .agg(
                count("*").alias("num_clientes"),
                avg("frecuencia").alias("avg_frecuencia"),
                avg("volumen_total").alias("avg_volumen_total"),
                avg("diversidad_productos").alias("avg_diversidad_productos"),
                avg("diversidad_categorias").alias("avg_diversidad_categorias"),
            )
            .orderBy("cluster")
            .collect()
        )

        clusters_resumen = []
        for r in resumen_rows:
            clusters_resumen.append(
                {
                    "cluster": int(r["cluster"]),
                    "num_clientes": int(r["num_clientes"]),
                    "porcentaje_clientes": round(
                        r["num_clientes"] / num_clientes * 100, 2
                    )
                    if num_clientes > 0
                    else 0.0,
                    "promedios": {
                        "frecuencia": float(round(r["avg_frecuencia"], 2))
                        if r["avg_frecuencia"] is not None
                        else None,
                        "volumen_total": float(round(r["avg_volumen_total"], 2))
                        if r["avg_volumen_total"] is not None
                        else None,
                        "diversidad_productos": float(
                            round(r["avg_diversidad_productos"], 2)
                        )
                        if r["avg_diversidad_productos"] is not None
                        else None,
                        "diversidad_categorias": float(
                            round(r["avg_diversidad_categorias"], 2)
                        )
                        if r["avg_diversidad_categorias"] is not None
                        else None,
                    },
                }
            )

        # Algunos ejemplos de clientes segmentados
        example_rows = (
            clustered.select("customer_id", "cluster")
            .orderBy("customer_id")
            .limit(50)
            .collect()
        )
        clientes_ejemplo = [
            {"customer_id": int(r["customer_id"]), "cluster": int(r["cluster"])}
            for r in example_rows
        ]

        result = {
            "k": k,
            "num_clientes": int(num_clientes),
            "feature_names": feature_cols,
            "clusters_resumen": clusters_resumen,
            "clientes_ejemplo": clientes_ejemplo,
        }

        ti.xcom_push(key="segmentacion_clientes", value=result)
    except Exception as e:
        logger.error(f"Error en segmentación de clientes: {e}")
        raise
    finally:
        spark.stop()

# =============================================================================
# TAREA 2: Recomendador de productos (reglas de asociación)
# =============================================================================
def generar_recomendador_productos(ti):
    """
    Genera reglas de asociación (FPGrowth) a partir de productos en cada transacción.
    Sirve como base para recomendador de productos.
    """

    logger.info("=" * 80)
    logger.info("TAREA 2 - RECOMENDADOR DE PRODUCTOS (FPGrowth)")
    logger.info("=" * 80)

    spark = (
        SparkSession.builder.appName("Transacciones_Recomendador_Productos")
        .master("local[*]")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )

    try:
        trans_path = os.path.join(DATASET_PATH, "Transactions")
        trans_files = [f for f in os.listdir(trans_path) if f.endswith("_Tran.csv")]
        if not trans_files:
            logger.warning("No se encontraron archivos de transacciones para reglas de asociación")
            ti.xcom_push(key="recomendador_productos", value={})
            return

        file_paths = [os.path.join(trans_path, f) for f in trans_files]
        df = spark.read.csv(file_paths, header=False, inferSchema=False, sep="|").toDF(
            "date", "store_id", "customer_id", "products"
        )

        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        df = df.withColumn("store_id", col("store_id").cast(IntegerType()))
        df = df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
        df = df.withColumn("productos_array", split(col("products"), " "))

        # FPGrowth
        fpGrowth = FPGrowth(
            itemsCol="productos_array", minSupport=0.02, minConfidence=0.3
        )
        model = fpGrowth.fit(df)

        reglas_df = (
            model.associationRules.select(
                "antecedent", "consequent", "confidence", "lift", "support"
            )
            .orderBy(col("lift").desc())
        )

        reglas = []
        for row in reglas_df.limit(100).collect():
            reglas.append(
                {
                    "antecedente": [str(x) for x in row["antecedent"]],
                    "consecuente": [str(x) for x in row["consequent"]],
                    "confidence": float(round(row["confidence"], 4)),
                    "lift": float(round(row["lift"], 4)),
                    "support": float(round(row["support"], 4)),
                }
            )

        result = {
            "min_support": 0.02,
            "min_confidence": 0.3,
            "num_reglas": len(reglas),
            "reglas": reglas,
        }

        ti.xcom_push(key="recomendador_productos", value=result)
    except Exception as e:
        logger.error(f"Error en recomendador de productos: {e}")
        raise
    finally:
        spark.stop()

# =============================================================================
# TAREA 3: Cohortes y retención
# =============================================================================
def calcular_cohortes_retencion(ti):
    """
    Calcula:
      - Tamaño de cohorte (mes de primera compra).
      - Matriz de retención por cohorte e índice de mes.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        to_date,
        trunc,
        min as smin,
        countDistinct,
        months_between,
        floor,
    )
    from pyspark.sql.types import IntegerType

    logger.info("=" * 80)
    logger.info("TAREA 3 - COHORTES Y RETENCIÓN")
    logger.info("=" * 80)

    spark = (
        SparkSession.builder.appName("Transacciones_Cohortes_Retencion")
        .master("local[*]")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .getOrCreate()
    )

    try:
        trans_path = os.path.join(DATASET_PATH, "Transactions")
        trans_files = [f for f in os.listdir(trans_path) if f.endswith("_Tran.csv")]
        if not trans_files:
            logger.warning("No se encontraron archivos de transacciones para cohortes")
            ti.xcom_push(key="cohortes_retencion", value={})
            return

        file_paths = [os.path.join(trans_path, f) for f in trans_files]
        df = spark.read.csv(file_paths, header=False, inferSchema=False, sep="|").toDF(
            "date", "store_id", "customer_id", "products"
        )

        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        df = df.withColumn("store_id", col("store_id").cast(IntegerType()))
        df = df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
        df = df.withColumn("cohort_month", trunc(col("date"), "MM"))

        # primera compra por cliente
        first_purchase = df.groupBy("customer_id").agg(
            smin("cohort_month").alias("first_month")
        )
        df = df.join(first_purchase, on="customer_id", how="left")
        df = df.withColumn(
            "cohort_index",
            floor(months_between(col("cohort_month"), col("first_month"))).cast("int"),
        )

        # tamaño por cohorte (clientes únicos en mes 0)
        cohort_sizes = (
            df.filter(col("cohort_index") == 0)
            .groupBy("first_month")
            .agg(countDistinct("customer_id").alias("cohort_size"))
        )

        # clientes activos por cohorte e índice
        active_by_index = (
            df.groupBy("first_month", "cohort_index")
            .agg(countDistinct("customer_id").alias("active_customers"))
        )

        joined = active_by_index.join(cohort_sizes, on="first_month", how="left")
        from pyspark.sql.functions import col as fcol

        retention = joined.withColumn(
            "retention_rate",
            (fcol("active_customers") / fcol("cohort_size")),
        )

        cohort_sizes_rows = cohort_sizes.orderBy("first_month").collect()
        cohort_sizes_list = [
            {
                "cohort_month": str(r["first_month"]),
                "cohort_size": int(r["cohort_size"]),
            }
            for r in cohort_sizes_rows
        ]

        retention_rows = retention.orderBy("first_month", "cohort_index").collect()
        retention_list = [
            {
                "cohort_month": str(r["first_month"]),
                "cohort_index": int(r["cohort_index"]),
                "active_customers": int(r["active_customers"]),
                "cohort_size": int(r["cohort_size"])
                if r["cohort_size"] is not None
                else None,
                "retention_rate": float(round(r["retention_rate"], 4))
                if r["retention_rate"] is not None
                else None,
            }
            for r in retention_rows
        ]

        result = {
            "cohort_sizes": cohort_sizes_list,
            "retention": retention_list,
        }

        ti.xcom_push(key="cohortes_retencion", value=result)
    except Exception as e:
        logger.error(f"Error en cohortes/retención: {e}")
        raise
    finally:
        spark.stop()

# =============================================================================
# TAREA 4: Forecast mensual (global y por tienda)
# =============================================================================
def calcular_forecast_mensual(ti, periods_ahead=6):
    """
    Pronóstico mensual de transacciones (global y por tienda).
    Usa ExponentialSmoothing si puede; si no, naive estacional por promedio de mes.
    """

    logger.info("=" * 80)
    logger.info("TAREA 4 - FORECAST MENSUAL (GLOBAL Y POR TIENDA)")
    logger.info("=" * 80)

    spark = (
        SparkSession.builder.appName("Transacciones_Forecast_Mensual")
        .master("local[*]")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .getOrCreate()
    )

    try:
        trans_path = os.path.join(DATASET_PATH, "Transactions")
        trans_files = [f for f in os.listdir(trans_path) if f.endswith("_Tran.csv")]
        if not trans_files:
            logger.warning("No se encontraron archivos de transacciones para forecast")
            ti.xcom_push(key="forecast_mensual", value={})
            return

        file_paths = [os.path.join(trans_path, f) for f in trans_files]
        df = spark.read.csv(file_paths, header=False, inferSchema=False, sep="|").toDF(
            "date", "store_id", "customer_id", "products"
        )

        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        df = df.withColumn("store_id", col("store_id").cast(IntegerType()))
        df = df.withColumn("month", trunc(col("date"), "MM"))

        monthly_global = (
            df.groupBy("month")
            .agg(count("*").alias("transactions"))
            .orderBy("month")
        )
        monthly_store = (
            df.groupBy("store_id", "month")
            .agg(count("*").alias("transactions"))
            .orderBy("store_id", "month")
        )

        def _forecast_series_pdf(pdf, is_global=False):
            """
            Recibe pandas DataFrame con columnas:
              - Global: ['month', 'transactions']
              - Por tienda: ['store_id', 'month', 'transactions']
            Retorna DataFrame con columnas:
              ['store_id'(opcional),'month','yhat','method']
            """
            import pandas as pd

            pdf = pdf.sort_values("month")
            idx_col = None
            if "store_id" in pdf.columns:
                idx_col = int(pdf["store_id"].iloc[0])

            ts = pdf.set_index("month")["transactions"].astype(float)
            ts = ts.asfreq("MS").fillna(0.0)

            yhat = None
            method = None
            try:
                seasonal = "add"
                trend = "add"
                seasonal_periods = 12
                if len(ts) >= max(18, seasonal_periods + 6):
                    model = ExponentialSmoothing(
                        ts,
                        trend=trend,
                        seasonal=seasonal,
                        seasonal_periods=seasonal_periods,
                        initialization_method="estimated",
                    )
                    fit = model.fit(optimized=True)
                    yhat = fit.forecast(periods_ahead)
                    method = "ExponentialSmoothing"
            except Exception as e:
                logger.warning(f"Fallo ES, usando naive estacional: {e}")
                method = None

            if yhat is None:
                grp = ts.groupby(ts.index.month).mean()
                future_idx = pd.date_range(
                    start=ts.index[-1]
                    + pd.offsets.MonthBegin(1),
                    periods=periods_ahead,
                    freq="MS",
                )
                yhat_vals = []
                for d in future_idx:
                    mu = grp.get(d.month, ts.mean())
                    yhat_vals.append(mu if pd.notnull(mu) else ts.mean())
                
                yhat = pd.Series(yhat_vals, index=future_idx)
                method = "Naive-Seasonal-Avg"

            out = yhat.to_frame(name="yhat").reset_index().rename(
                columns={"index": "month"}
            )
            if idx_col is not None:
                out.insert(0, "store_id", idx_col)
            out["method"] = method
            return out

        # Global
        pdf_global = monthly_global.toPandas()
        if pdf_global.empty:
            logger.warning("No hay datos mensuales globales para forecast")
            ti.xcom_push(key="forecast_mensual", value={})
            spark.stop()
            return

        fcst_global = _forecast_series_pdf(pdf_global, is_global=True)

        # Por tienda
        pdf_store = monthly_store.toPandas()
        import pandas as pd

        fcst_stores_list = []
        for sid, g in pdf_store.groupby("store_id"):
            fcst_stores_list.append(_forecast_series_pdf(g))
        fcst_store = (
            pd.concat(fcst_stores_list, ignore_index=True)
            if fcst_stores_list
            else pd.DataFrame(columns=["store_id", "month", "yhat", "method"])
        )

        forecast_global = [
            {
                "month": str(r["month"].date()),
                "yhat": float(r["yhat"]),
                "method": r["method"],
            }
            for _, r in fcst_global.iterrows()
        ]
        forecast_tiendas = [
            {
                "store_id": int(r["store_id"]),
                "month": str(r["month"].date()),
                "yhat": float(r["yhat"]),
                "method": r["method"],
            }
            for _, r in fcst_store.iterrows()
        ]

        result = {
            "horizon_months": periods_ahead,
            "global": forecast_global,
            "tiendas": forecast_tiendas,
        }

        ti.xcom_push(key="forecast_mensual", value=result)
    except Exception as e:
        logger.error(f"Error en forecasting mensual: {e}")
        raise
    finally:
        spark.stop()

# =============================================================================
# TAREA 5: Generar JSON final de modelos avanzados
# =============================================================================
def generar_json_modelos_avanzados(ti):
    logger.info("=" * 80)
    logger.info("TAREA 5 - GENERAR JSON MODELOS AVANZADOS")
    logger.info("=" * 80)

    try:
        seg = ti.xcom_pull(
            key="segmentacion_clientes", task_ids="segmentar_clientes_kmeans"
        )
        rec = ti.xcom_pull(
            key="recomendador_productos", task_ids="generar_recomendador_productos"
        )
        coh = ti.xcom_pull(
            key="cohortes_retencion", task_ids="calcular_cohortes_retencion"
        )
        fc = ti.xcom_pull(
            key="forecast_mensual", task_ids="calcular_forecast_mensual"
        )

        modelos = {
            "segmentacion_clientes": seg if seg else {},
            "recomendador_productos": rec if rec else {},
            "cohortes_retencion": coh if coh else {},
            "forecast_mensual": fc if fc else {},
        }

        os.makedirs(REPORTS_PATH, exist_ok=True)
        output_path = os.path.join(
            REPORTS_PATH, "transacciones_modelos_avanzados.json"
        )
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(modelos, f, indent=4, ensure_ascii=False)

        logger.info(f"JSON de modelos avanzados generado en: {output_path}")
    except Exception as e:
        logger.error(f"Error generando JSON de modelos avanzados: {e}")
        raise

# =============================================================================
# DEFINICIÓN DEL DAG
# =============================================================================
with DAG(
    "transacciones_modelos_avanzados",
    default_args=default_args,
    description="Modelos avanzados sobre transacciones: segmentación, recomendador, cohortes y forecast",
    schedule_interval=None,
    catchup=False,
    tags=["transacciones", "modelos_avanzados", "analytics"],
) as dag:

    tarea_segmentacion = PythonOperator(
        task_id="segmentar_clientes_kmeans",
        python_callable=segmentar_clientes_kmeans,
    )

    # tarea_recomendador = PythonOperator(
    #     task_id="generar_recomendador_productos",
    #     python_callable=generar_recomendador_productos,
    # )

    tarea_cohortes = PythonOperator(
        task_id="calcular_cohortes_retencion",
        python_callable=calcular_cohortes_retencion,
    )

    tarea_forecast = PythonOperator(
        task_id="calcular_forecast_mensual",
        python_callable=calcular_forecast_mensual,
    )

    tarea_json_avanzados = PythonOperator(
        task_id="generar_json_modelos_avanzados",
        python_callable=generar_json_modelos_avanzados,
    )

    [tarea_segmentacion, tarea_cohortes, tarea_forecast] >> tarea_json_avanzados