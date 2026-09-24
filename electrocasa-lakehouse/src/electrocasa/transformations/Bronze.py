# ============================================================
# ElectroCasa - Bronze Layer
# ============================================================
#
# Metodos de ingesta utilizados:
#
# 1. Auto Loader:
#    - ventas_sucursales.csv
#    - empleados_rrhh.csv
#    - resenas_clientes.json
#    - devoluciones.csv
#
# 2. COPY INTO:
#    - catalogo_productos.json
#
# 3. Lakehouse Federation:
#    - Azure SQL dbo.TrackingEnvios
#
# La capa Bronze conserva los datos con la menor transformacion
# posible. La limpieza y normalizacion se realizara en Silver.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURACION
# ============================================================

spark = SparkSession.builder.getOrCreate()

CATALOG = "electrocasa"
BRONZE_SCHEMA = "bronze"

LANDING_PATH = "/Volumes/electrocasa/bronze/landing"

# Estado persistente de Auto Loader.
# La ficha exige que schemaLocation y checkpointLocation
# vivan bajo un Volume y no en almacenamiento efimero.
STATE_PATH = f"{LANDING_PATH}/_autoloader"

SCHEMA_ROOT = f"{STATE_PATH}/schemas"
CHECKPOINT_ROOT = f"{STATE_PATH}/checkpoints"


# ============================================================
# FUNCION GENERICA PARA AUTO LOADER
# ============================================================

def cargar_autoloader(
    archivo,
    formato,
    tabla_destino,
    estado,
    multiline=False
):
    """
    Ingesta incremental mediante Auto Loader.

    Cada fuente utiliza:
      - schemaLocation independiente
      - checkpointLocation independiente

    Ambos se almacenan bajo el Unity Catalog Volume.
    """

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option(
            "cloudFiles.schemaLocation",
            f"{SCHEMA_ROOT}/{estado}"
        )
        .option(
            "pathGlobFilter",
            archivo
        )
        .option(
            "cloudFiles.inferColumnTypes",
            "true"
        )
    )

    # Configuracion especifica para CSV
    if formato.lower() == "csv":
        reader = reader.option("header", "true")

    # Los JSON del proyecto son multilínea
    if multiline:
        reader = reader.option("multiLine", "true")

    df = (
        reader
        .load(LANDING_PATH)

        # Metadata tecnica del archivo de origen
        .select(
            "*",

            F.col("_metadata.file_path")
            .alias("_source_file"),

            F.col("_metadata.file_name")
            .alias("_source_file_name"),

            F.col("_metadata.file_modification_time")
            .alias("_source_file_modification_time")
        )

        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )

        .withColumn(
            "_source",
            F.lit(archivo)
        )
    )

    tabla_completa = (
        f"{CATALOG}.{BRONZE_SCHEMA}.{tabla_destino}"
    )

    print(
        f"Iniciando Auto Loader: "
        f"{archivo} -> {tabla_completa}"
    )

    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")

        .option(
            "checkpointLocation",
            f"{CHECKPOINT_ROOT}/{estado}"
        )

        .option(
            "mergeSchema",
            "true"
        )

        # Procesa todo lo disponible y termina.
        # Ideal para ejecutarlo como tarea de un Job.
        .trigger(availableNow=True)

        .toTable(tabla_completa)
    )

    query.awaitTermination()

    print(
        f"OK - Auto Loader completado: {tabla_completa}"
    )


# ============================================================
# 1. VENTAS - AUTO LOADER
# ============================================================
#
# Justificacion:
# Las ventas son datos transaccionales que pueden recibirse
# continuamente desde las sucursales.
#
# Auto Loader permite detectar archivos nuevos de forma
# incremental sin reprocesar continuamente todo el directorio.
# ============================================================

cargar_autoloader(
    archivo="ventas_sucursales.csv",
    formato="csv",
    tabla_destino="bronze_ventas",
    estado="ventas"
)


# ============================================================
# 2. EMPLEADOS RRHH - AUTO LOADER
# ============================================================
#
# Justificacion:
# El archivo contiene eventos historicos de empleados
# (alta, baja, transferencia y cambios salariales).
#
# Nuevos eventos pueden incorporarse periodicamente, por lo que
# se utiliza ingestion incremental con Auto Loader.
# ============================================================

cargar_autoloader(
    archivo="empleados_rrhh.csv",
    formato="csv",
    tabla_destino="bronze_empleados",
    estado="empleados"
)


# ============================================================
# 3. RESENAS CLIENTES - AUTO LOADER
# ============================================================
#
# Justificacion:
# Las resenas representan eventos generados continuamente por
# clientes y contienen estructuras JSON anidadas como tags y
# respuestas.
#
# Auto Loader permite incorporarlas incrementalmente mientras
# Bronze conserva la estructura original.
# ============================================================

cargar_autoloader(
    archivo="resenas_clientes.json",
    formato="json",
    tabla_destino="bronze_resenas",
    estado="resenas",
    multiline=True
)


# ============================================================
# 4. DEVOLUCIONES - AUTO LOADER
# ============================================================
#
# Justificacion:
# Las devoluciones son eventos operativos/transaccionales que
# pueden llegar periodicamente.
#
# Auto Loader mantiene el estado de los archivos procesados
# mediante checkpoint.
# ============================================================

cargar_autoloader(
    archivo="devoluciones.csv",
    formato="csv",
    tabla_destino="bronze_devoluciones",
    estado="devoluciones"
)


# ============================================================
# 5. PRODUCTOS - COPY INTO
# ============================================================
#
# Justificacion:
# El catalogo de productos se comporta como un snapshot de baja
# frecuencia y volumen moderado.
#
# COPY INTO resulta apropiado para cargas batch ocasionales.
# Ademas es idempotente: Databricks registra los archivos ya
# cargados y no los vuelve a insertar al reejecutar la carga.
# ============================================================

print("Iniciando COPY INTO: catalogo_productos.json")


spark.sql(
    """
    CREATE TABLE IF NOT EXISTS
        electrocasa.bronze.bronze_productos
    (
        producto_id STRING,
        nombre_producto STRING,
        categoria STRING,
        marca STRING,
        precio_lista STRING,

        _source_file STRING,
        _ingestion_timestamp TIMESTAMP,
        _source STRING
    )
    USING DELTA
    """
)


resultado_copy = spark.sql(
    f"""
    COPY INTO electrocasa.bronze.bronze_productos

    FROM
    (
        SELECT

            CAST(producto_id AS STRING)
                AS producto_id,

            CAST(nombre_producto AS STRING)
                AS nombre_producto,

            CAST(categoria AS STRING)
                AS categoria,

            CAST(marca AS STRING)
                AS marca,

            CAST(precio_lista AS STRING)
                AS precio_lista,

            _metadata.file_path
                AS _source_file,

            current_timestamp()
                AS _ingestion_timestamp,

            'catalogo_productos.json'
                AS _source

        FROM '{LANDING_PATH}'
    )

    FILEFORMAT = JSON

    FILES = (
        'catalogo_productos.json'
    )

    FORMAT_OPTIONS (
        'multiLine' = 'true'
    )

    COPY_OPTIONS (
        'force' = 'false'
    )
    """
)


print("Resultado COPY INTO:")
resultado_copy.show(
    truncate=False
)

print(
    "OK - COPY INTO productos completado"
)


# ============================================================
# 6. TRACKING ENVIOS - LAKEHOUSE FEDERATION
# ============================================================
#
# Justificacion:
# Tracking de envios ya reside en Azure SQL Database.
#
# Lakehouse Federation permite acceder directamente al sistema
# fuente mediante Unity Catalog sin almacenar credenciales en
# este archivo.
#
# Solo se proyectan las columnas necesarias para el caso.
#
# IMPORTANTE:
# La conexion final debe utilizar Databricks Secret Scope.
# Ningun usuario/password debe quedar en este codigo ni GitHub.
# ============================================================

print(
    "Iniciando lectura federada de Azure SQL..."
)


df_tracking = (
    spark
    .table(
        "electrocasa_sql.dbo.trackingenvios"
    )

    # Solo las columnas necesarias
    .select(
        "tracking_id",
        "pedido_id",
        "courier",
        "estado_entrega",
        "sucursal_origen",
        "fecha_actualizacion"
    )

    .withColumn(
        "_ingestion_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "_source",
        F.lit(
            "azure_sql_lakehouse_federation"
        )
    )

    .withColumn(
        "_source_table",
        F.lit(
            "electrocasa_sql.dbo.trackingenvios"
        )
    )
)


# Tracking se trata como snapshot del origen operativo.
# Cada ejecucion deja Bronze sincronizado con la fuente.
(
    df_tracking
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "electrocasa.bronze.bronze_tracking"
    )
)


print(
    "OK - Tracking cargado mediante "
    "Lakehouse Federation"
)


# ============================================================
# VALIDACION FINAL
# ============================================================

print("")
print("==============================================")
print("VALIDACION DE CAPA BRONZE")
print("==============================================")


tablas_bronze = [
    "bronze_ventas",
    "bronze_empleados",
    "bronze_resenas",
    "bronze_devoluciones",
    "bronze_productos",
    "bronze_tracking"
]


for tabla in tablas_bronze:

    nombre_completo = (
        f"{CATALOG}."
        f"{BRONZE_SCHEMA}."
        f"{tabla}"
    )

    total = (
        spark
        .table(nombre_completo)
        .count()
    )

    print(
        f"{nombre_completo}: "
        f"{total} registros"
    )


print("==============================================")
print("INGESTA BRONZE FINALIZADA CORRECTAMENTE")
print("==============================================")