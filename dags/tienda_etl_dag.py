from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import json
import os
from pathlib import Path

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

def revision_inicial_tienda(ti):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, isnan, count, when
    
    logger.info("=" * 80)
    logger.info("REVISIÓN INICIAL - TABLAS DE PRODUCTOS")
    logger.info("=" * 80)
    
    spark = SparkSession.builder \
        .appName("Tienda_RevisionInicial") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    resultados = {}
    
    try:
        categories_path = os.path.join(DATASET_PATH, "Products", "Categories.csv")
        product_category_path = os.path.join(DATASET_PATH, "Products", "ProductCategory.csv")
        
        # ==================== CATEGORIES ====================
        logger.info("Analizando: Categories.csv")
        df_cat = spark.read.csv(categories_path, header=True, inferSchema=True, sep="|")
        
        num_filas_cat = df_cat.count()
        num_columnas_cat = len(df_cat.columns)
        
        tipos_datos_cat = {field.name: str(field.dataType) for field in df_cat.schema.fields}
        
        nulos_cat = {}
        for columna in df_cat.columns:
            col_escaped = f"`{columna}`"
            if str(df_cat.schema[columna].dataType) in ['IntegerType()', 'LongType()', 'DoubleType()', 'FloatType()']:
                nulos = df_cat.filter(col(col_escaped).isNull() | isnan(col(col_escaped))).count()
            else:
                nulos = df_cat.filter(col(col_escaped).isNull() | (col(col_escaped) == "")).count()
            nulos_cat[columna] = nulos
        
        num_duplicados_cat = num_filas_cat - df_cat.dropDuplicates().count()
        
        resultados["Categories"] = {
            "archivo": "Products/Categories.csv",
            "num_filas": num_filas_cat,
            "num_columnas": num_columnas_cat,
            "columnas": df_cat.columns,
            "tipos_datos": tipos_datos_cat,
            "valores_nulos": nulos_cat,
            "total_nulos": sum(nulos_cat.values()),
            "num_duplicados": num_duplicados_cat,
            "porcentaje_duplicados": round(num_duplicados_cat / num_filas_cat * 100, 2) if num_filas_cat > 0 else 0
        }
        
        logger.info(f"  -> Registros: {num_filas_cat}")
        logger.info(f"  -> Columnas: {num_columnas_cat}")
        logger.info(f"  -> Nulos totales: {sum(nulos_cat.values())}")
        logger.info(f"  -> Duplicados: {num_duplicados_cat}")
        
        # ==================== PRODUCT CATEGORY ====================
        logger.info("Analizando: ProductCategory.csv")
        df_prod = spark.read.csv(product_category_path, header=True, inferSchema=True, sep="|")
        
        num_filas_prod = df_prod.count()
        num_columnas_prod = len(df_prod.columns)
        
        tipos_datos_prod = {field.name: str(field.dataType) for field in df_prod.schema.fields}
        
        nulos_prod = {}
        for columna in df_prod.columns:
            col_escaped = f"`{columna}`"
            if str(df_prod.schema[columna].dataType) in ['IntegerType()', 'LongType()', 'DoubleType()', 'FloatType()']:
                nulos = df_prod.filter(col(col_escaped).isNull() | isnan(col(col_escaped))).count()
            else:
                nulos = df_prod.filter(col(col_escaped).isNull() | (col(col_escaped) == "")).count()
            nulos_prod[columna] = nulos
        
        num_duplicados_prod = num_filas_prod - df_prod.dropDuplicates().count()
        
        resultados["ProductCategory"] = {
            "archivo": "Products/ProductCategory.csv",
            "num_filas": num_filas_prod,
            "num_columnas": num_columnas_prod,
            "columnas": df_prod.columns,
            "tipos_datos": tipos_datos_prod,
            "valores_nulos": nulos_prod,
            "total_nulos": sum(nulos_prod.values()),
            "num_duplicados": num_duplicados_prod,
            "porcentaje_duplicados": round(num_duplicados_prod / num_filas_prod * 100, 2) if num_filas_prod > 0 else 0
        }
        
        logger.info(f"  -> Registros: {num_filas_prod}")
        logger.info(f"  -> Columnas: {num_columnas_prod}")
        logger.info(f"  -> Nulos totales: {sum(nulos_prod.values())}")
        logger.info(f"  -> Duplicados: {num_duplicados_prod}")
        
        os.makedirs(REPORTS_PATH, exist_ok=True)
        output_path = os.path.join(REPORTS_PATH, "tienda_revision_inicial.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(resultados, f, indent=4, ensure_ascii=False)
        
        logger.info(f"Revisión inicial guardada en: {output_path}")
        ti.xcom_push(key="revision_inicial", value=resultados)
        
    except Exception as e:
        logger.error(f"Error en revisión inicial: {e}")
        raise
    finally:
        spark.stop()


# ================================================================================
# TAREA 2: Estadísticas Descriptivas - ProductCategory
# ================================================================================
def estadisticas_product_category(ti):
    """
    Estadísticas descriptivas de ProductCategory:
    - Frecuencias y distribuciones de códigos de producto
    - Frecuencias y distribuciones de códigos de categoría
    - Análisis de relaciones producto-categoría
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, countDistinct, desc
    
    logger.info("=" * 80)
    logger.info("ESTADÍSTICAS DESCRIPTIVAS - PRODUCT CATEGORY")
    logger.info("=" * 80)
    
    spark = SparkSession.builder \
        .appName("Tienda_EstadisticasProductCategory") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    estadisticas = {}
    
    try:
        # Cargar datos
        product_category_path = os.path.join(DATASET_PATH, "Products", "ProductCategory.csv")
        categories_path = os.path.join(DATASET_PATH, "Products", "Categories.csv")
        
        df_prod = spark.read.csv(product_category_path, header=True, inferSchema=True, sep="|")
        df_cat = spark.read.csv(categories_path, header=True, inferSchema=True, sep="|")
        
        total_registros = df_prod.count()
        
        # ==================== ANÁLISIS DE CÓDIGOS DE PRODUCTO ====================
        logger.info("Analizando códigos de producto...")
        
        productos_unicos = df_prod.select("`v.code_pr`").distinct().count()        
        
        estadisticas["productos"] = {
            "total_registros": total_registros,
            "productos_unicos": productos_unicos,
            "productos_duplicados": total_registros - productos_unicos
        }
        
        logger.info(f"  -> Total registros: {total_registros}")
        logger.info(f"  -> Productos únicos: {productos_unicos}")
        logger.info(f"  -> Productos duplicados: {total_registros - productos_unicos}")

        # ==================== ANÁLISIS DE CÓDIGOS DE CATEGORÍA ====================
        logger.info("Analizando códigos de categoría...")
        
        categorias_unicas = df_prod.select("`v.code`").distinct().count()
        
        cat_distribution = df_prod.groupBy("`v.code`") \
            .agg(count("*").alias("num_productos")) \
            .orderBy(desc("num_productos")) \
            .collect()
        
        cat_stats = []
        for row in cat_distribution:
            codigo_cat = row["v.code"]
            num_prod = row["num_productos"]
            porcentaje = round((num_prod / total_registros) * 100, 2)
            
            nombre_cat = df_cat.filter(col("id") == codigo_cat).select("category_name").first()
            nombre = nombre_cat["category_name"] if nombre_cat else "Desconocida"
            
            cat_stats.append({
                "codigo_categoria": codigo_cat,
                "nombre_categoria": nombre,
                "num_productos": num_prod,
                "porcentaje": porcentaje
            })
        
        estadisticas["categorias"] = {
            "categorias_unicas": categorias_unicas,
            "distribucion_categorias": cat_stats
        }
        
        logger.info(f"  -> Categorías únicas: {categorias_unicas}")
        
        # ==================== ANÁLISIS DE RELACIONES ====================
        logger.info("Analizando relaciones producto-categoría...")
        
        # Productos que pertenecen a múltiples categorías
        productos_multi_cat = df_prod.groupBy("`v.code_pr`") \
            .agg(countDistinct("`v.code`").alias("num_categorias")) \
            .filter(col("num_categorias") > 1) \
            .count()
        
        # Promedio de categorías por producto
        avg_cat_por_producto = df_prod.groupBy("`v.code_pr`") \
            .agg(countDistinct("`v.code`").alias("num_categorias")) \
            .agg({"num_categorias": "avg"}) \
            .collect()[0][0]
        
        # Promedio de productos por categoría
        avg_prod_por_categoria = df_prod.groupBy("`v.code`") \
            .agg(countDistinct("`v.code_pr`").alias("num_productos")) \
            .agg({"num_productos": "avg"}) \
            .collect()[0][0]
        
        estadisticas["relaciones"] = {
            "productos_en_multiples_categorias": productos_multi_cat,
            "porcentaje_productos_multi_categoria": round((productos_multi_cat / productos_unicos) * 100, 2),
            "promedio_categorias_por_producto": round(avg_cat_por_producto, 2),
            "promedio_productos_por_categoria": round(avg_prod_por_categoria, 2)
        }
        
        logger.info(f"  -> Productos en múltiples categorías: {productos_multi_cat}")
        logger.info(f"  -> Promedio categorías/producto: {round(avg_cat_por_producto, 2)}")
        
        os.makedirs(REPORTS_PATH, exist_ok=True)
        output_path = os.path.join(REPORTS_PATH, "tienda_estadisticas_product_category.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(estadisticas, f, indent=4, ensure_ascii=False)
        
        logger.info(f"Estadísticas guardadas en: {output_path}")
        ti.xcom_push(key="estadisticas", value=estadisticas)
        
    except Exception as e:
        logger.error(f"Error en estadísticas: {e}")
        raise
    finally:
        spark.stop()


# ================================================================================
# TAREA 3: Generar Reporte Consolidado
# ================================================================================
def generar_reporte_consolidado(ti):
    """
    Genera un reporte consolidado en Excel y CSV con todos los análisis
    """
    import pandas as pd
    
    logger.info("=" * 80)
    logger.info("GENERANDO REPORTE CONSOLIDADO")
    logger.info("=" * 80)
    
    try:
        # Obtener datos de tareas anteriores
        revision = ti.xcom_pull(key="revision_inicial", task_ids="revision_inicial_tienda")
        estadisticas = ti.xcom_pull(key="estadisticas", task_ids="estadisticas_product_category")
        
        if not revision or not estadisticas:
            logger.warning("No se encontraron datos de tareas anteriores")
            return
        
        # ==================== HOJA 1: RESUMEN GENERAL ====================
        resumen_data = []
        
        for tabla, info in revision.items():
            resumen_data.append({
                "Tabla": tabla,
                "Archivo": info.get("archivo", ""),
                "Registros": info.get("num_filas", 0),
                "Columnas": info.get("num_columnas", 0),
                "Total Nulos": info.get("total_nulos", 0),
                "Duplicados": info.get("num_duplicados", 0),
                "% Duplicados": info.get("porcentaje_duplicados", 0)
            })
        
        df_resumen = pd.DataFrame(resumen_data)
        
        # ==================== HOJA 2: DISTRIBUCIÓN DE CATEGORÍAS ====================
        df_categorias = pd.DataFrame(estadisticas["categorias"]["distribucion_categorias"])
                
        # ==================== HOJA 3: ANÁLISIS DE RELACIONES ====================
        relaciones_data = [{
            "Métrica": key,
            "Valor": value
        } for key, value in estadisticas["relaciones"].items()]
        df_relaciones = pd.DataFrame(relaciones_data)
        
        # Guardar en Excel
        excel_path = os.path.join(REPORTS_PATH, "Tienda_Reporte_Completo.xlsx")
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            df_resumen.to_excel(writer, sheet_name="Resumen General", index=False)
            df_categorias.to_excel(writer, sheet_name="Distribución Categorías", index=False)
            df_relaciones.to_excel(writer, sheet_name="Análisis Relaciones", index=False)
        
        logger.info(f"Reporte Excel generado: {excel_path}")
        
        # Guardar CSV del resumen
        csv_path = os.path.join(REPORTS_PATH, "Tienda_Resumen.csv")
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
    'tienda_analisis_productos',
    default_args=default_args,
    description='Análisis EDA de tablas Categories y ProductCategory',
    schedule_interval=None,
    catchup=False,
    tags=['tienda', 'eda', 'productos']
) as dag:
    
    tarea_1 = PythonOperator(
        task_id='revision_inicial_tienda',
        python_callable=revision_inicial_tienda
    )
    
    tarea_2 = PythonOperator(
        task_id='estadisticas_product_category',
        python_callable=estadisticas_product_category
    )
    
    tarea_3 = PythonOperator(
        task_id='generar_reporte_consolidado',
        python_callable=generar_reporte_consolidado
    )
    
    tarea_1 >> tarea_2 >> tarea_3