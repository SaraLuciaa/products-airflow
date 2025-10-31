# 🛒 Análisis EDA de Datos de Retail con Apache Airflow

Proyecto de **Análisis Exploratorio de Datos (EDA)** para datos de retail utilizando Apache Airflow y PySpark. El sistema procesa archivos CSV de productos, categorías y transacciones para generar reportes estadísticos completos.

---

## 🔧 Requisitos

- Docker Desktop instalado y corriendo
- Docker Compose
- 8GB de RAM disponible (mínimo)
- Puertos disponibles: 8082 (Airflow Web), 5432 (PostgreSQL)

---

## 📁 Estructura del Proyecto

```
airflow-SaraLuciaaa/
├── dags/
│   ├── tienda_etl_dag.py           # DAG análisis Productos y Categorías
│   └── transacciones_eda_dag.py    # DAG análisis Transacciones
├── dataset/
│   ├── Products/
│   │   ├── Categories.csv          # Catálogo de categorías
│   │   └── ProductCategory.csv     # Relación producto-categoría
│   └── Transactions/
│       ├── 102_Tran.csv           # Transacciones tienda 102
│       ├── 103_Tran.csv           # Transacciones tienda 103
│       ├── 107_Tran.csv           # Transacciones tienda 107
│       └── 110_Tran.csv           # Transacciones tienda 110
├── reports/                        # Reportes generados automáticamente
├── logs/                          # Logs de Airflow
├── docker-compose.yml
├── Dockerfile
├── env.compose
├── requirements.txt
└── README.md
```

---

## 🚀 Cómo Ejecutar el Proyecto

### 1. Clonar el Repositorio

```bash
git clone https://github.com/SaraLuciaa/products-airflow.git
cd products-airflow
```

### 2. Construir la Imagen de Docker

```bash
docker-compose --env-file env.compose build
```

Este proceso puede tardar varios minutos la primera vez, ya que instala todas las dependencias necesarias (Apache Airflow, PySpark, pandas, openpyxl, etc.).

### 3. Levantar los Contenedores

```bash
docker-compose --env-file env.compose up -d
```

Esto iniciará los siguientes servicios:
- **Airflow Webserver** (Puerto 8082)
- **Airflow Scheduler**
- **PostgreSQL** (Base de datos de metadatos)
- **Airflow Init** (Inicialización)

### 4. Acceder a la Interfaz Web de Airflow

1. Abrir el navegador en: **http://localhost:8082**
2. Credenciales de acceso:
   - **Usuario:** `admin`
   - **Contraseña:** `admin`

### 5. Ejecutar los DAGs

Una vez dentro de la interfaz:

1. **Activar el DAG** usando el toggle (switch) a la izquierda
2. **Ejecutar el DAG** haciendo clic en el botón "Play" ▶️ (Trigger DAG)
3. **Monitorear el progreso** en la vista de Grid o Graph
4. **Ver logs** haciendo clic en cada tarea

### 6. Acceder a los Reportes

Los reportes se generan automáticamente en la carpeta `reports/`:

```bash
# Ver reportes generados
ls reports/
```

También puedes acceder desde el contenedor:

```bash
docker exec -it airflow-saraluciaaa-copia-webserver-1 ls /opt/airflow/reports
```

### 7. Detener los Contenedores

```bash
docker-compose down
```

Para eliminar también los volúmenes:

```bash
docker-compose down -v
```