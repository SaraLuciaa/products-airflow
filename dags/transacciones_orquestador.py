# dags/transacciones_orquestador.py

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.models import Variable

from datetime import datetime, timedelta
import os
import json
import logging

logger = logging.getLogger(__name__)

DATASET_PATH = "/opt/airflow/dataset/Transactions"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 24),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

SNAPSHOT_VAR = "transactions_snapshot"  # nombre de la Variable en Airflow


def calcular_snapshot():
    """
    Construye un snapshot de la carpeta Transactions, basado en:
    - nombre del archivo
    - fecha de modificación (mtime)

    Devuelve un string JSON ordenado, para poder compararlo fácilmente
    con lo guardado en la Variable.
    """
    if not os.path.exists(DATASET_PATH):
        logger.warning(f"La ruta {DATASET_PATH} no existe.")
        return json.dumps({}, sort_keys=True)

    files = [
        f
        for f in os.listdir(DATASET_PATH)
        if f.endswith("_Tran.csv")
    ]
    info = {}
    for f in files:
        full = os.path.join(DATASET_PATH, f)
        try:
            mtime = os.path.getmtime(full)
            info[f] = mtime
        except FileNotFoundError:
            # Por si se borra mientras listamos
            continue

    # Lo devolvemos como string JSON ordenado para compararlo
    snapshot_str = json.dumps(info, sort_keys=True)
    logger.info(f"Snapshot actual: {snapshot_str}")
    return snapshot_str


def detectar_cambios(**context):
    """
    Compara el snapshot actual con el último guardado en Variable.
    Si hay cambios, actualiza la Variable y devuelve la lista de task_ids
    que deben ejecutarse (los TriggerDagRun).
    Si no hay cambios, devuelve el task_id de 'no_hay_nuevos_datos'.
    """
    # Snapshot actual
    current_snapshot = calcular_snapshot()

    # Snapshot previo guardado en Variable (si no existe, consideramos que hay cambios)
    last_snapshot = Variable.get(SNAPSHOT_VAR, default_var=None)

    logger.info(f"Snapshot previo: {last_snapshot}")
    logger.info(f"Snapshot nuevo : {current_snapshot}")

    if last_snapshot is None or last_snapshot != current_snapshot:
        logger.info("Se detectaron nuevos o modificados archivos de transacciones. Actualizando snapshot.")
        Variable.set(SNAPSHOT_VAR, current_snapshot)
        # Disparamos los DAGs de análisis
        return [
            "disparar_eda",
            "disparar_modelos_avanzados",
            "disparar_tercer_dag",   # Ajusta este nombre al DAG real
        ]
    else:
        logger.info("No se detectaron cambios en los archivos de transacciones.")
        return "no_hay_nuevos_datos"


with DAG(
    "transacciones_orquestador",
    default_args=default_args,
    description="Orquestador que detecta nuevos datos y dispara los DAGs de análisis",
    schedule_interval="*/10 * * * *",   
    catchup=False,
    tags=["transacciones", "orquestador"],
    max_active_runs=1
) as dag:

    decidir_disparo = BranchPythonOperator(
        task_id="detectar_cambios_y_decidir",
        python_callable=detectar_cambios,
        provide_context=True,
    )

    # Si hay datos nuevos -> se activan estos
    disparar_eda = TriggerDagRunOperator(
        task_id="disparar_eda",
        trigger_dag_id="transacciones_eda_basica",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    disparar_modelos_avanzados = TriggerDagRunOperator(
        task_id="disparar_modelos_avanzados",
        trigger_dag_id="transacciones_modelos_avanzados",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    disparar_etl_products = TriggerDagRunOperator(
        task_id="disparar_etl_products",
        trigger_dag_id="tienda_etl_dag",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    no_hay_nuevos_datos = EmptyOperator(
        task_id="no_hay_nuevos_datos"
    )

    decidir_disparo >> disparar_eda >> disparar_modelos_avanzados >> disparar_etl_products
