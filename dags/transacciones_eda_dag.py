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
# TAREA 1: Revisión Inicial del Dataset de Transacciones
# ================================================================================
def revision_inicial_transacciones(ti):
    """
    Revisión inicial de los archivos de transacciones:
    - Número de registros y columnas
    - Tipos de datos
    - Valores faltantes o nulos
    - Duplicados
    - Análisis de estructura de productos
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, isnan, count, when, length, size, split
    from pyspark.sql.types import StringType, IntegerType, DateType
    
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
        # Buscar todos los archivos de transacciones
        trans_path = os.path.join(DATASET_PATH, "Transactions")
        trans_files = [f for f in os.listdir(trans_path) if f.endswith("_Tran.csv")]
        
        logger.info(f"Archivos de transacciones encontrados: {len(trans_files)}")
        
        for archivo in trans_files:
            logger.info(f"\nAnalizando: {archivo}")
            file_path = os.path.join(trans_path, archivo)
            
            # Leer CSV sin header (los archivos no tienen encabezado)
            df = spark.read.csv(file_path, header=False, inferSchema=False, sep="|")
            
            # Asignar nombres de columnas
            df = df.toDF("date", "store_id", "customer_id", "products")
            
            # Convertir tipos de datos apropiados
            from pyspark.sql.functions import to_date
            df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
            df = df.withColumn("store_id", col("store_id").cast(IntegerType()))
            df = df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
            
            num_filas = df.count()
            num_columnas = len(df.columns)
            
            logger.info(f"  -> Total de transacciones: {num_filas:,}")
            
            # Tipos de datos
            tipos_datos = {field.name: str(field.dataType) for field in df.schema.fields}
            
            # ==================== VALORES NULOS ====================
            nulos_por_columna = {}
            for columna in ["date", "store_id", "customer_id", "products"]:
                if columna == "products":
                    # Para productos, verificar vacíos o nulls
                    nulos = df.filter(
                        col(columna).isNull() | 
                        (col(columna) == "") | 
                        (length(col(columna)) == 0)
                    ).count()
                else:
                    nulos = df.filter(col(columna).isNull()).count()
                nulos_por_columna[columna] = nulos
            
            # ==================== DUPLICADOS ====================
            # Duplicados exactos (toda la fila)
            num_duplicados_exactos = num_filas - df.dropDuplicates().count()
            
            # Duplicados por cliente-fecha (mismo cliente comprando varias veces el mismo día)
            num_duplicados_cliente_fecha = num_filas - df.dropDuplicates(["date", "customer_id"]).count()
            
            # ==================== ANÁLISIS DE PRODUCTOS ====================
            # Añadir columna con el conteo de productos por transacción
            df = df.withColumn("productos_array", split(col("products"), " "))
            df = df.withColumn("num_productos", size(col("productos_array")))
            
            # Estadísticas de productos por transacción
            from pyspark.sql.functions import avg, min, max, stddev
            stats_productos = df.select(
                avg("num_productos").alias("promedio"),
                min("num_productos").alias("minimo"),
                max("num_productos").alias("maximo"),
                stddev("num_productos").alias("desv_std")
            ).collect()[0]
            
            # ==================== ANÁLISIS TEMPORAL ====================
            from pyspark.sql.functions import year, month, dayofweek
            df = df.withColumn("year", year(col("date")))
            df = df.withColumn("month", month(col("date")))
            
            rango_fechas = df.select(
                min("date").alias("fecha_min"),
                max("date").alias("fecha_max")
            ).collect()[0]
            
            # ==================== GUARDAR RESULTADOS ====================
            store_id = archivo.split("_")[0]
            
            resultados[store_id] = {
                "archivo": archivo,
                "estructura": {
                    "num_transacciones": num_filas,
                    "num_columnas": num_columnas,
                    "columnas": df.columns[:4],  # Solo las 4 principales
                    "tipos_datos": tipos_datos
                },
                "calidad_datos": {
                    "valores_nulos": nulos_por_columna,
                    "total_nulos": sum(nulos_por_columna.values()),
                    "duplicados_exactos": num_duplicados_exactos,
                    "duplicados_cliente_fecha": num_duplicados_cliente_fecha,
                    "porcentaje_duplicados_exactos": round(num_duplicados_exactos / num_filas * 100, 2),
                },
                "analisis_productos": {
                    "productos_por_transaccion": {
                        "promedio": round(stats_productos["promedio"], 2),
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
            
            logger.info(f"  -> Nulos totales: {sum(nulos_por_columna.values())}")
            logger.info(f"  -> Duplicados exactos: {num_duplicados_exactos}")
            logger.info(f"  -> Productos/transacción (promedio): {stats_productos['promedio']:.2f}")
        
        # Guardar resultados
        os.makedirs(REPORTS_PATH, exist_ok=True)
        output_path = os.path.join(REPORTS_PATH, "transacciones_revision_inicial.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(resultados, f, indent=4, ensure_ascii=False)
        
        logger.info(f"\nRevisión inicial guardada en: {output_path}")
        ti.xcom_push(key="revision_inicial", value=resultados)
        
    except Exception as e:
        logger.error(f"Error en revisión inicial: {e}")
        raise
    finally:
        spark.stop()


# ================================================================================
# TAREA 2: Estadísticas Descriptivas de Transacciones
# ================================================================================
def estadisticas_descriptivas_transacciones(ti):
    """
    Estadísticas descriptivas de las transacciones:
    - Variables numéricas: customer_id, num_productos, store_id
    - Variables categóricas: productos más comprados, distribución temporal
    - Análisis de comportamiento de compra
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, count, countDistinct, avg, min, max, stddev, 
        percentile_approx, desc, explode, split, year, month, 
        dayofweek, to_date, size
    )
    from pyspark.sql.types import IntegerType
    
    logger.info("=" * 80)
    logger.info("ESTADÍSTICAS DESCRIPTIVAS - TRANSACCIONES")
    logger.info("=" * 80)
    
    spark = SparkSession.builder \
        .appName("Transacciones_Estadisticas") \
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
        
        for archivo in trans_files:
            logger.info(f"\nProcesando: {archivo}")
            file_path = os.path.join(trans_path, archivo)
            store_id = archivo.split("_")[0]
            
            # Leer y preparar datos
            df = spark.read.csv(file_path, header=False, inferSchema=False, sep="|")
            df = df.toDF("date", "store_id", "customer_id", "products")
            
            df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
            df = df.withColumn("store_id", col("store_id").cast(IntegerType()))
            df = df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
            df = df.withColumn("productos_array", split(col("products"), " "))
            df = df.withColumn("num_productos", size(col("productos_array")))
            
            total_transacciones = df.count()
            
            # ==================== ESTADÍSTICAS NUMÉRICAS ====================
            logger.info("  -> Calculando estadísticas numéricas...")
            
            estadisticas_numericas = {}
            
            # CUSTOMER_ID - Cache el dataframe para reutilizarlo
            df.cache()
            
            clientes_unicos = df.select("customer_id").distinct().count()
            
            stats_customer = df.select(
                avg("customer_id").alias("media"),
                stddev("customer_id").alias("desv_std"),
                min("customer_id").alias("minimo"),
                max("customer_id").alias("maximo")
            ).collect()[0]
            
            # Percentiles con accuracy menor para mejor performance
            percentiles = df.select(
                percentile_approx("customer_id", [0.25, 0.5, 0.75], 10000).alias("percentiles")
            ).collect()[0]
            
            p25, mediana, p75 = percentiles["percentiles"]
            
            # Moda de customer_id (top 1)
            moda_customer = df.groupBy("customer_id").count() \
                .orderBy(desc("count")).first()
            
            # Outliers de customer_id
            iqr = p75 - p25
            outliers_customer = df.filter(
                (col("customer_id") < p25 - 1.5 * iqr) | 
                (col("customer_id") > p75 + 1.5 * iqr)
            ).count()
            
            estadisticas_numericas["customer_id"] = {
                "clientes_unicos": clientes_unicos,
                "media": round(stats_customer["media"], 2) if stats_customer["media"] else 0,
                "mediana": int(mediana) if mediana else 0,
                "moda": moda_customer["customer_id"] if moda_customer else None,
                "desviacion_estandar": round(stats_customer["desv_std"], 2) if stats_customer["desv_std"] else 0,
                "minimo": stats_customer["minimo"],
                "maximo": stats_customer["maximo"],
                "percentil_25": int(p25) if p25 else 0,
                "percentil_50_mediana": int(mediana) if mediana else 0,
                "percentil_75": int(p75) if p75 else 0,
                "rango_intercuartil_iqr": round(iqr, 2) if iqr else 0,
                "outliers_count": outliers_customer
            }
            
            # NUM_PRODUCTOS (productos por transacción)
            stats_num_prod = df.select(
                avg("num_productos").alias("media"),
                stddev("num_productos").alias("desv_std"),
                min("num_productos").alias("minimo"),
                max("num_productos").alias("maximo")
            ).collect()[0]
            
            # Percentiles con accuracy menor
            percentiles_np = df.select(
                percentile_approx("num_productos", [0.25, 0.5, 0.75], 10000).alias("percentiles")
            ).collect()[0]
            
            p25_np, mediana_np, p75_np = percentiles_np["percentiles"]
            
            moda_num_prod = df.groupBy("num_productos").count() \
                .orderBy(desc("count")).first()
            
            iqr_np = p75_np - p25_np
            outliers_num_prod = df.filter(
                (col("num_productos") < p25_np - 1.5 * iqr_np) | 
                (col("num_productos") > p75_np + 1.5 * iqr_np)
            ).count()
            
            estadisticas_numericas["num_productos_por_transaccion"] = {
                "media": round(stats_num_prod["media"], 2) if stats_num_prod["media"] else 0,
                "mediana": int(mediana_np) if mediana_np else 0,
                "moda": moda_num_prod["num_productos"] if moda_num_prod else None,
                "desviacion_estandar": round(stats_num_prod["desv_std"], 2) if stats_num_prod["desv_std"] else 0,
                "minimo": stats_num_prod["minimo"],
                "maximo": stats_num_prod["maximo"],
                "percentil_25": int(p25_np) if p25_np else 0,
                "percentil_50_mediana": int(mediana_np) if mediana_np else 0,
                "percentil_75": int(p75_np) if p75_np else 0,
                "rango_intercuartil_iqr": round(iqr_np, 2) if iqr_np else 0,
                "outliers_count": outliers_num_prod
            }
            
            # ==================== ESTADÍSTICAS CATEGÓRICAS ====================
            logger.info("  -> Calculando estadísticas categóricas...")
            
            estadisticas_categoricas = {}
            
            # TOP PRODUCTOS MÁS COMPRADOS
            logger.info("    -> Analizando productos individuales...")
            
            df_productos_exploded = df.select(
                explode(split(col("products"), " ")).alias("product_id")
            )
            
            # Convertir product_id a integer y filtrar nulos
            df_productos_exploded = df_productos_exploded.withColumn(
                "product_id", col("product_id").cast(IntegerType())
            ).filter(col("product_id").isNotNull())
            
            # Cache para reutilizar
            df_productos_exploded.cache()
            
            total_productos_vendidos = df_productos_exploded.count()
            logger.info(f"    -> Total productos vendidos: {total_productos_vendidos:,}")
            
            # Top 30 productos más vendidos
            top_productos = df_productos_exploded.groupBy("product_id") \
                .count() \
                .withColumnRenamed("count", "frecuencia") \
                .orderBy(desc("frecuencia")) \
                .limit(30) \
                .collect()
            
            productos_data = []
            for row in top_productos:
                producto_id = row["product_id"]
                frecuencia = row["frecuencia"]
                porcentaje = round((frecuencia / total_productos_vendidos) * 100, 2)
                productos_data.append({
                    "product_id": producto_id,
                    "frecuencia_absoluta": frecuencia,
                    "frecuencia_relativa_pct": porcentaje
                })
            
            estadisticas_categoricas["top_30_productos_mas_vendidos"] = {
                "total_productos_vendidos": total_productos_vendidos,
                "productos_unicos": df_productos_exploded.select("product_id").distinct().count(),
                "top_30": productos_data
            }
            
            # DISTRIBUCIÓN TEMPORAL - Por mes
            df = df.withColumn("year", year(col("date")))
            df = df.withColumn("month", month(col("date")))
            df = df.withColumn("dia_semana", dayofweek(col("date")))
            
            dist_mensual = df.groupBy("year", "month") \
                .agg(count("*").alias("num_transacciones")) \
                .orderBy("year", "month") \
                .collect()
            
            mensual_data = []
            for row in dist_mensual:
                num_trans = row["num_transacciones"]
                porcentaje = round((num_trans / total_transacciones) * 100, 2)
                mensual_data.append({
                    "year": row["year"],
                    "month": row["month"],
                    "num_transacciones": num_trans,
                    "porcentaje": porcentaje
                })
            
            estadisticas_categoricas["distribucion_temporal_mensual"] = mensual_data
            
            # DISTRIBUCIÓN POR DÍA DE LA SEMANA
            dist_semanal = df.groupBy("dia_semana") \
                .agg(count("*").alias("num_transacciones")) \
                .orderBy("dia_semana") \
                .collect()
            
            dias_semana = {1: "Domingo", 2: "Lunes", 3: "Martes", 4: "Miércoles", 
                          5: "Jueves", 6: "Viernes", 7: "Sábado"}
            
            semanal_data = []
            for row in dist_semanal:
                num_trans = row["num_transacciones"]
                porcentaje = round((num_trans / total_transacciones) * 100, 2)
                semanal_data.append({
                    "dia_semana": dias_semana.get(row["dia_semana"], "Desconocido"),
                    "num_transacciones": num_trans,
                    "porcentaje": porcentaje
                })
            
            estadisticas_categoricas["distribucion_dia_semana"] = semanal_data
            
            # FRECUENCIA DE COMPRA POR CLIENTE
            compras_por_cliente = df.groupBy("customer_id") \
                .agg(count("*").alias("num_compras")) \
                .select("num_compras")
            
            freq_compras = compras_por_cliente.groupBy("num_compras") \
                .agg(count("*").alias("num_clientes")) \
                .orderBy(desc("num_clientes")) \
                .limit(20) \
                .collect()
            
            freq_data = []
            for row in freq_compras:
                num_compras = row["num_compras"]
                num_clientes = row["num_clientes"]
                porcentaje = round((num_clientes / clientes_unicos) * 100, 2)
                freq_data.append({
                    "num_compras": num_compras,
                    "num_clientes": num_clientes,
                    "porcentaje_clientes": porcentaje
                })
            
            estadisticas_categoricas["frecuencia_compra_clientes"] = {
                "top_20_frecuencias": freq_data,
                "descripcion": "Número de clientes según cuántas veces compraron"
            }
            
            # ==================== GUARDAR RESULTADOS ====================
            estadisticas[store_id] = {
                "archivo": archivo,
                "total_transacciones": total_transacciones,
                "estadisticas_numericas": estadisticas_numericas,
                "estadisticas_categoricas": estadisticas_categoricas
            }
            
            logger.info(f"  -> Clientes únicos: {clientes_unicos}")
            logger.info(f"  -> Productos únicos vendidos: {estadisticas_categoricas['top_30_productos_mas_vendidos']['productos_unicos']}")
            
            # Liberar memoria
            df.unpersist()
            df_productos_exploded.unpersist()
            logger.info(f"  Completado análisis de tienda {store_id}")
        
        # Guardar resultados
        os.makedirs(REPORTS_PATH, exist_ok=True)
        output_path = os.path.join(REPORTS_PATH, "transacciones_estadisticas_descriptivas.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(estadisticas, f, indent=4, ensure_ascii=False)
        
        logger.info(f"\nEstadísticas guardadas en: {output_path}")
        ti.xcom_push(key="estadisticas", value=estadisticas)
        
    except Exception as e:
        logger.error(f"Error en estadísticas: {e}")
        raise
    finally:
        spark.stop()


# ================================================================================
# TAREA 3: Generar Reporte Consolidado
# ================================================================================
def generar_reporte_consolidado_transacciones(ti):
    """
    Genera reportes consolidados en Excel y CSV
    """
    import pandas as pd
    
    logger.info("=" * 80)
    logger.info("GENERANDO REPORTE CONSOLIDADO - TRANSACCIONES")
    logger.info("=" * 80)
    
    try:
        revision = ti.xcom_pull(key="revision_inicial", task_ids="revision_inicial_transacciones")
        estadisticas = ti.xcom_pull(key="estadisticas", task_ids="estadisticas_descriptivas_transacciones")
        
        if not revision or not estadisticas:
            logger.warning("No se encontraron datos de tareas anteriores")
            return
        
        # ==================== HOJA 1: RESUMEN GENERAL ====================
        resumen_data = []
        for store_id, info in revision.items():
            est = estadisticas.get(store_id, {})
            resumen_data.append({
                "Tienda_ID": store_id,
                "Archivo": info["archivo"],
                "Total_Transacciones": info["estructura"]["num_transacciones"],
                "Clientes_Unicos": est.get("estadisticas_numericas", {}).get("customer_id", {}).get("clientes_unicos", 0),
                "Fecha_Inicio": info["rango_temporal"]["fecha_inicio"],
                "Fecha_Fin": info["rango_temporal"]["fecha_fin"],
                "Productos_Promedio_Transaccion": info["analisis_productos"]["productos_por_transaccion"]["promedio"],
                "Duplicados": info["calidad_datos"]["duplicados_exactos"],
                "Nulos": info["calidad_datos"]["total_nulos"]
            })
        
        df_resumen = pd.DataFrame(resumen_data)
        
        # Crear Excel con múltiples hojas
        excel_path = os.path.join(REPORTS_PATH, "Transacciones_Reporte_Completo.xlsx")
        
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            df_resumen.to_excel(writer, sheet_name="Resumen General", index=False)
            
            # Por cada tienda, crear hojas adicionales
            for store_id, est in estadisticas.items():
                # Top productos
                if "estadisticas_categoricas" in est and "top_30_productos_mas_vendidos" in est["estadisticas_categoricas"]:
                    df_productos = pd.DataFrame(
                        est["estadisticas_categoricas"]["top_30_productos_mas_vendidos"]["top_30"]
                    )
                    sheet_name = f"Top_Productos_{store_id}"[:31]  # Límite de 31 caracteres
                    df_productos.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Distribución mensual
                if "estadisticas_categoricas" in est and "distribucion_temporal_mensual" in est["estadisticas_categoricas"]:
                    df_mensual = pd.DataFrame(
                        est["estadisticas_categoricas"]["distribucion_temporal_mensual"]
                    )
                    sheet_name = f"Dist_Mensual_{store_id}"[:31]
                    df_mensual.to_excel(writer, sheet_name=sheet_name, index=False)
        
        logger.info(f"Reporte Excel generado: {excel_path}")
        
        # Guardar CSV del resumen
        csv_path = os.path.join(REPORTS_PATH, "Transacciones_Resumen.csv")
        df_resumen.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info(f"Reporte CSV generado: {csv_path}")
        
        logger.info("=" * 80)
        logger.info("REPORTE CONSOLIDADO COMPLETADO")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error generando reporte: {e}")
        raise


# ================================================================================
# DEFINICIÓN DEL DAG
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
    
    tarea_3 = PythonOperator(
        task_id='generar_reporte_consolidado_transacciones',
        python_callable=generar_reporte_consolidado_transacciones
    )
    
    # Definir dependencias
    tarea_1 >> tarea_2 >> tarea_3
