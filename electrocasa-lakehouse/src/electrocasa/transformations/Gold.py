# ============================================================
# ElectroCasa - Gold Layer
# ============================================================
#
# La capa Gold consume exclusivamente información limpia
# proveniente de Silver.
#
# Objetivos:
# - generar indicadores de negocio
# - facilitar reporting y dashboards
# - evitar que los consumidores tengan que rehacer joins
#   o agregaciones complejas
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


CATALOG = spark.conf.get("electrocasa.catalog")
SILVER_SCHEMA = spark.conf.get("electrocasa.silver_schema")
GOLD_SCHEMA = spark.conf.get("electrocasa.gold_schema")


def silver_table(nombre):
    return f"{CATALOG}.{SILVER_SCHEMA}.{nombre}"


def gold_table(nombre):
    return f"{CATALOG}.{GOLD_SCHEMA}.{nombre}"


# ============================================================
# 1. VENTAS POR SUCURSAL
# ============================================================

@dp.materialized_view(
    name=gold_table("gold_ventas_sucursal"),
    comment="KPIs comerciales agregados por sucursal"
)
def gold_ventas_sucursal():

    ventas = spark.read.table(
        silver_table("silver_ventas")
    )

    return (
        ventas
        .groupBy("sucursal_id")
        .agg(
            F.countDistinct("venta_id").alias("total_ventas"),

            F.sum("cantidad").alias("unidades_vendidas"),

            F.round(
                F.sum("monto_total"),
                2
            ).alias("ingresos_totales"),

            F.round(
                F.avg("monto_total"),
                2
            ).alias("ticket_promedio"),

            F.min("fecha_venta").alias("primera_venta"),

            F.max("fecha_venta").alias("ultima_venta")
        )
    )


# ============================================================
# 2. VENTAS MENSUALES
# ============================================================

@dp.materialized_view(
    name=gold_table("gold_ventas_mensuales"),
    comment="Evolución mensual de ventas e ingresos"
)
def gold_ventas_mensuales():

    ventas = spark.read.table(
        silver_table("silver_ventas")
    )

    return (
        ventas
        .withColumn(
            "mes",
            F.date_trunc(
                "month",
                F.col("fecha_venta")
            ).cast("date")
        )
        .groupBy("mes")
        .agg(
            F.countDistinct("venta_id").alias("total_ventas"),

            F.sum("cantidad").alias("unidades_vendidas"),

            F.round(
                F.sum("monto_total"),
                2
            ).alias("ingresos_totales"),

            F.round(
                F.avg("monto_total"),
                2
            ).alias("ticket_promedio")
        )
        .orderBy("mes")
    )


# ============================================================
# 3. RENDIMIENTO POR CATEGORIA
# ============================================================

@dp.materialized_view(
    name=gold_table("gold_ventas_categoria"),
    comment="Indicadores comerciales por categoría de producto"
)
def gold_ventas_categoria():

    ventas = spark.read.table(
        silver_table("silver_ventas")
    )

    productos = spark.read.table(
        silver_table("silver_productos")
    )

    productos_dim = productos.select(
        "producto_id",
        "categoria",
        "marca",
        "nombre_producto"
    )

    df = (
        ventas
        .join(
            productos_dim,
            on="producto_id",
            how="left"
        )
        .withColumn(
            "categoria",
            F.coalesce(
                F.col("categoria"),
                F.lit("SIN_CATALOGO")
            )
        )
    )

    return (
        df
        .groupBy("categoria")
        .agg(
            F.countDistinct("venta_id").alias("total_ventas"),

            F.countDistinct("producto_id").alias("productos_distintos"),

            F.sum("cantidad").alias("unidades_vendidas"),

            F.round(
                F.sum("monto_total"),
                2
            ).alias("ingresos_totales"),

            F.round(
                F.avg("monto_total"),
                2
            ).alias("ticket_promedio")
        )
    )


# ============================================================
# 4. SATISFACCION DE CLIENTES POR PRODUCTO
# ============================================================

@dp.materialized_view(
    name=gold_table("gold_satisfaccion_producto"),
    comment="KPIs de satisfacción y reseñas por producto"
)
def gold_satisfaccion_producto():

    resenas = spark.read.table(
        silver_table("silver_resenas")
    )

    productos = spark.read.table(
        silver_table("silver_productos")
    )

    df = (
        resenas
        .join(
            productos.select(
                "producto_id",
                "nombre_producto",
                "categoria",
                "marca"
            ),
            on="producto_id",
            how="left"
        )
    )

    return (
        df
        .groupBy(
            "producto_id",
            "nombre_producto",
            "categoria",
            "marca"
        )
        .agg(
            F.countDistinct(
                "resena_id"
            ).alias("total_resenas"),

            F.round(
                F.avg("calificacion"),
                2
            ).alias("calificacion_promedio"),

            F.sum(
                F.when(
                    F.col("calificacion") <= 2,
                    1
                ).otherwise(0)
            ).alias("resenas_negativas"),

            F.sum(
                F.when(
                    F.col("calificacion") >= 4,
                    1
                ).otherwise(0)
            ).alias("resenas_positivas")
        )
        .withColumn(
            "pct_resenas_negativas",
            F.round(
                F.col("resenas_negativas")
                / F.col("total_resenas")
                * 100,
                2
            )
        )
        .withColumn(
            "pct_resenas_positivas",
            F.round(
                F.col("resenas_positivas")
                / F.col("total_resenas")
                * 100,
                2
            )
        )
    )


# ============================================================
# 5. DEVOLUCIONES POR PRODUCTO
# ============================================================

@dp.materialized_view(
    name=gold_table("gold_devoluciones_producto"),
    comment="Indicadores de devoluciones y reembolsos por producto"
)
def gold_devoluciones_producto():

    devoluciones = spark.read.table(
        silver_table("silver_devoluciones")
    )

    productos = spark.read.table(
        silver_table("silver_productos")
    )

    df = (
        devoluciones
        .join(
            productos.select(
                "producto_id",
                "nombre_producto",
                "categoria",
                "marca"
            ),
            on="producto_id",
            how="left"
        )
    )

    return (
        df
        .groupBy(
            "producto_id",
            "nombre_producto",
            "categoria",
            "marca"
        )
        .agg(
            F.countDistinct(
                "devolucion_id"
            ).alias("total_devoluciones"),

            F.round(
                F.sum("monto_reembolso"),
                2
            ).alias("reembolso_total"),

            F.round(
                F.avg("monto_reembolso"),
                2
            ).alias("reembolso_promedio")
        )
    )


# ============================================================
# 6. MOTIVOS DE DEVOLUCION
# ============================================================

@dp.materialized_view(
    name=gold_table("gold_motivos_devolucion"),
    comment="Distribución de devoluciones según motivo"
)
def gold_motivos_devolucion():

    devoluciones = spark.read.table(
        silver_table("silver_devoluciones")
    )

    return (
        devoluciones
        .groupBy("motivo")
        .agg(
            F.countDistinct(
                "devolucion_id"
            ).alias("total_devoluciones"),

            F.round(
                F.sum("monto_reembolso"),
                2
            ).alias("reembolso_total"),

            F.round(
                F.avg("monto_reembolso"),
                2
            ).alias("reembolso_promedio")
        )
    )


# ============================================================
# 7. TRACKING POR COURIER Y ESTADO
# ============================================================

@dp.materialized_view(
    name=gold_table("gold_tracking_courier"),
    comment="Distribución de envíos por courier y estado"
)
def gold_tracking_courier():

    tracking = spark.read.table(
        silver_table("silver_tracking")
    )

    return (
        tracking
        .groupBy(
            "courier",
            "estado_entrega"
        )
        .agg(
            F.countDistinct(
                "tracking_id"
            ).alias("total_envios"),

            F.max(
                "fecha_actualizacion"
            ).alias("ultima_actualizacion")
        )
    )


# ============================================================
# 8. DOTACION ACTUAL POR SUCURSAL
# ============================================================

@dp.materialized_view(
    name=gold_table("gold_dotacion_sucursal"),
    comment="Dotación actual estimada de empleados por sucursal"
)
def gold_dotacion_sucursal():

    empleados = spark.read.table(
        silver_table("silver_empleados")
    )

    ventana = (
        Window
        .partitionBy("id_empleado")
        .orderBy(
            F.col("fecha_evento").desc(),
            F.col("_silver_timestamp").desc()
        )
    )

    actuales = (
        empleados
        .withColumn(
            "_rn",
            F.row_number().over(ventana)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .filter(
            F.col("tipo_evento") != "BAJA"
        )
    )

    return (
        actuales
        .groupBy("sucursal_id")
        .agg(
            F.countDistinct(
                "id_empleado"
            ).alias("empleados_activos"),

            F.round(
                F.avg("salario"),
                2
            ).alias("salario_promedio"),

            F.round(
                F.sum("salario"),
                2
            ).alias("masa_salarial")
        )
    )