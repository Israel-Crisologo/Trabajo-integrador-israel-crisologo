# ============================================================
# ElectroCasa - Silver Layer
# ============================================================
#
# La capa Silver:
# - tipifica columnas
# - normaliza valores de texto
# - elimina registros inválidos de las tablas Silver
# - conserva los registros rechazados en cuarentena
# - controla duplicados
# - conserva el historial cuando corresponde
#
# Las reglas se basan en los hallazgos realizados durante
# la exploración de datos.
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def normalizar_codigo(columna):
    """
    Convierte textos a un formato comparable:
    - trim
    - elimina tildes
    - mayúsculas
    - espacios y guiones -> _
    """

    limpio = F.translate(
        F.trim(columna),
        "áéíóúüñÁÉÍÓÚÜÑ",
        "aeiouunAEIOUUN"
    )

    limpio = F.upper(limpio)

    limpio = F.regexp_replace(
        limpio,
        r"[^A-Z0-9]+",
        "_"
    )

    limpio = F.regexp_replace(
        limpio,
        r"^_+|_+$",
        ""
    )

    return limpio


# ============================================================
# 1. SILVER VENTAS
# ============================================================
#
# Hallazgos exploración:
# - sucursal_id con nulos
# - monto_total con nulos / <= 0
# - cantidad <= 0
# - venta_id duplicados
# - metodo_pago con distintas formas:
#   efectivo / EFECTIVO / EFV
#   tarjeta / TC / Tarjeta de credito
#   Yape / yape / YAPE
# ============================================================

@dp.materialized_view(
    name="silver_ventas",
    comment="Ventas limpias, tipificadas, normalizadas y deduplicadas"
)
@dp.expect_all_or_drop({
    "venta_id_valido": "venta_id IS NOT NULL",
    "sucursal_valida": "sucursal_id IS NOT NULL",
    "producto_valido": "producto_id IS NOT NULL",
    "cantidad_positiva": "cantidad > 0",
    "monto_positivo": "monto_total > 0",
    "fecha_valida": "fecha_venta IS NOT NULL"
})
def silver_ventas():

    df = spark.read.table(
        "electrocasa.bronze.bronze_ventas"
    )

    metodo_normalizado = normalizar_codigo(
        F.col("metodo_pago")
    )

    df = (
        df

        .withColumn(
            "cantidad",
            F.col("cantidad").cast("int")
        )

        .withColumn(
            "monto_total",
            F.col("monto_total").cast("double")
        )

        .withColumn(
            "fecha_venta",
            F.to_date("fecha_venta")
        )

        .withColumn(
            "metodo_pago_raw",
            F.col("metodo_pago")
        )

        .withColumn(
            "metodo_pago",
            F.when(
                metodo_normalizado.isin(
                    "EFECTIVO",
                    "EFV"
                ),
                F.lit("EFECTIVO")
            )
            .when(
                metodo_normalizado.isin(
                    "TC",
                    "TARJETA",
                    "TARJETA_CREDITO",
                    "TARJETA_DE_CREDITO"
                ),
                F.lit("TARJETA_CREDITO")
            )
            .when(
                metodo_normalizado.isin(
                    "TRANSFERENCIA",
                    "TRANSFERENCIA_BANCARIA"
                ),
                F.lit("TRANSFERENCIA")
            )
            .when(
                metodo_normalizado == "YAPE",
                F.lit("YAPE")
            )
            .when(
                metodo_normalizado == "PLIN",
                F.lit("PLIN")
            )
            .otherwise(metodo_normalizado)
        )

        .withColumn(
            "canal",
            normalizar_codigo(
                F.col("canal")
            )
        )

        .withColumn(
            "_silver_timestamp",
            F.current_timestamp()
        )
    )

    # venta_id debe representar una venta única.
    # Si aparece repetida conservamos la última versión ingerida.

    ventana = (
        Window
        .partitionBy("venta_id")
        .orderBy(
            F.col(
                "_ingestion_timestamp"
            ).desc_nulls_last(),

            F.col(
                "_source_file_modification_time"
            ).desc_nulls_last()
        )
    )

    return (
        df
        .withColumn(
            "_rn",
            F.row_number().over(ventana)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================
# 2. SILVER PRODUCTOS
# ============================================================
#
# Hallazgos exploración:
# - categorias escritas de muchas formas
# - marcas nulas
# - precios negativos / cero
# - producto_id duplicados
# - algunos IDs poseen precios distintos
#
# El origen no contiene fecha de vigencia del precio.
# Para resolver duplicados utilizamos una regla técnica
# determinista:
#
# 1. última ingesta
# 2. ante empate, mayor precio positivo
#
# Esta decisión se documentará en README.
# ============================================================

@dp.materialized_view(
    name="silver_productos",
    comment="Catálogo de productos normalizado y deduplicado"
)
@dp.expect_all_or_drop({
    "producto_id_valido": "producto_id IS NOT NULL",
    "nombre_valido": "nombre_producto IS NOT NULL",
    "categoria_valida": "categoria IS NOT NULL",
    "precio_positivo": "precio_lista > 0"
})
def silver_productos():

    df = spark.read.table(
        "electrocasa.bronze.bronze_productos"
    )

    precio = (
        F.regexp_replace(
            F.col("precio_lista"),
            r"[^0-9.\-]",
            ""
        )
        .cast("double")
    )

    df = (
        df

        .withColumn(
            "precio_lista",
            precio
        )

        .withColumn(
            "categoria_raw",
            F.col("categoria")
        )

        .withColumn(
            "categoria",
            normalizar_codigo(
                F.col("categoria")
            )
        )

        .withColumn(
            "marca",
            F.when(
                F.col("marca").isNull(),
                F.lit("DESCONOCIDA")
            )
            .otherwise(
                F.initcap(
                    F.lower(
                        F.trim("marca")
                    )
                )
            )
        )

        .withColumn(
            "nombre_producto",
            F.trim("nombre_producto")
        )

        .withColumn(
            "_silver_timestamp",
            F.current_timestamp()
        )
    )

    ventana = (
        Window
        .partitionBy("producto_id")
        .orderBy(
            F.col(
                "_ingestion_timestamp"
            ).desc_nulls_last(),

            F.col(
                "precio_lista"
            ).desc_nulls_last()
        )
    )

    return (
        df
        .withColumn(
            "_rn",
            F.row_number().over(ventana)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================
# 3. SILVER EMPLEADOS
# ============================================================
#
# Hallazgos:
# - un empleado puede aparecer varias veces
# - esto NO es necesariamente un duplicado:
#   representa historial:
#       ALTA
#       BAJA
#       TRANSFERENCIA
#       CAMBIO_SALARIO
#
# Por eso NO deduplicamos por id_empleado.
# Conservamos eventos históricos.
#
# Encontramos también:
# - DNI nulos
# - email nulos
# - fecha_evento nula
# - variantes de mayúsculas/minúsculas en tipo_evento
# ============================================================

@dp.materialized_view(
    name="silver_empleados",
    comment="Historial limpio de eventos de empleados"
)
@dp.expect_all_or_drop({
    "empleado_valido": "id_empleado IS NOT NULL",
    "nombre_valido": "nombre IS NOT NULL",
    "sucursal_valida": "sucursal_id IS NOT NULL",
    "salario_positivo": "salario > 0",
    "fecha_evento_valida": "fecha_evento IS NOT NULL",
    "tipo_evento_valido":
        "tipo_evento IN ('ALTA','BAJA','TRANSFERENCIA','CAMBIO_SALARIO')"
})
def silver_empleados():

    df = spark.read.table(
        "electrocasa.bronze.bronze_empleados"
    )

    df = (
        df

        .withColumn(
            "dni",
            F.col("dni").cast("string")
        )

        .withColumn(
            "email",
            F.lower(
                F.trim("email")
            )
        )

        .withColumn(
            "salario",
            F.col("salario").cast("double")
        )

        .withColumn(
            "fecha_evento",
            F.to_date("fecha_evento")
        )

        .withColumn(
            "tipo_evento_raw",
            F.col("tipo_evento")
        )

        .withColumn(
            "tipo_evento",
            normalizar_codigo(
                F.col("tipo_evento")
            )
        )

        .withColumn(
            "nombre",
            F.initcap(
                F.lower(
                    F.trim("nombre")
                )
            )
        )

        .withColumn(
            "cargo",
            F.initcap(
                F.lower(
                    F.trim("cargo")
                )
            )
        )

        .withColumn(
            "_silver_timestamp",
            F.current_timestamp()
        )
    )

    # Quitamos solamente eventos exactamente repetidos.
    # NO eliminamos el historial del empleado.

    return df.dropDuplicates([
        "id_empleado",
        "sucursal_id",
        "tipo_evento",
        "fecha_evento",
        "salario"
    ])


# ============================================================
# 4. SILVER RESENAS
# ============================================================
#
# Hallazgos:
# - calificacion nula
# - valores 0 y 6
# - rango válido esperado: 1 a 5
# - fecha_resena nula
# - resena_id duplicado
# - tags y respuestas son estructuras anidadas
# ============================================================

@dp.materialized_view(
    name="silver_resenas",
    comment="Reseñas válidas entre 1 y 5 estrellas, deduplicadas"
)
@dp.expect_all_or_drop({
    "resena_valida": "resena_id IS NOT NULL",
    "producto_valido": "producto_id IS NOT NULL",
    "cliente_valido": "cliente_id IS NOT NULL",
    "calificacion_valida":
        "calificacion BETWEEN 1 AND 5",
    "fecha_resena_valida":
        "fecha_resena IS NOT NULL"
})
def silver_resenas():

    df = spark.read.table(
        "electrocasa.bronze.bronze_resenas"
    )

    df = (
        df

        .withColumn(
            "calificacion",
            F.col("calificacion").cast("int")
        )

        .withColumn(
            "fecha_resena",
            F.to_date("fecha_resena")
        )

        .withColumn(
            "comentario",
            F.trim("comentario")
        )

        .withColumn(
            "tags",
            F.expr(
                """
                transform(
                    coalesce(
                        tags,
                        cast(array() as array<string>)
                    ),
                    x -> lower(trim(x))
                )
                """
            )
        )

        .withColumn(
            "_silver_timestamp",
            F.current_timestamp()
        )
    )

    ventana = (
        Window
        .partitionBy("resena_id")
        .orderBy(
            F.col(
                "fecha_resena"
            ).desc_nulls_last(),

            F.col(
                "_ingestion_timestamp"
            ).desc_nulls_last()
        )
    )

    return (
        df
        .withColumn(
            "_rn",
            F.row_number().over(ventana)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================
# 5. SILVER DEVOLUCIONES
# ============================================================
#
# Hallazgos:
# - pedido_id nulo
# - motivo nulo
# - monto_reembolso negativo
# - devolucion_id duplicado
# ============================================================

@dp.materialized_view(
    name="silver_devoluciones",
    comment="Devoluciones válidas y deduplicadas"
)
@dp.expect_all_or_drop({
    "devolucion_valida": "devolucion_id IS NOT NULL",
    "pedido_valido": "pedido_id IS NOT NULL",
    "producto_valido": "producto_id IS NOT NULL",
    "sucursal_valida": "sucursal_id IS NOT NULL",
    "motivo_valido": "motivo IS NOT NULL",
    "reembolso_positivo": "monto_reembolso > 0",
    "fecha_valida": "fecha_devolucion IS NOT NULL"
})
def silver_devoluciones():

    df = spark.read.table(
        "electrocasa.bronze.bronze_devoluciones"
    )

    df = (
        df

        .withColumn(
            "monto_reembolso",
            F.col(
                "monto_reembolso"
            ).cast("double")
        )

        .withColumn(
            "fecha_devolucion",
            F.to_date(
                "fecha_devolucion"
            )
        )

        .withColumn(
            "motivo_raw",
            F.col("motivo")
        )

        .withColumn(
            "motivo",
            normalizar_codigo(
                F.col("motivo")
            )
        )

        .withColumn(
            "_silver_timestamp",
            F.current_timestamp()
        )
    )

    ventana = (
        Window
        .partitionBy("devolucion_id")
        .orderBy(
            F.col(
                "fecha_devolucion"
            ).desc_nulls_last(),

            F.col(
                "_ingestion_timestamp"
            ).desc_nulls_last()
        )
    )

    return (
        df
        .withColumn(
            "_rn",
            F.row_number().over(ventana)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================
# 6. SILVER TRACKING
# ============================================================
#
# Hallazgos:
# - tracking_id duplicado
# - fecha_actualizacion nula
# - courier escrito con diferentes formatos
# - estado_entrega:
#       EN_CAMINO
#       En camino
#       en_transito
#       ENTREGADO
#       entregado
#       Pendiente
#       PENDIENTE
#       DEVUELTO
#       Devuelto
#
# Para cada tracking conservamos el estado más reciente.
# ============================================================

@dp.materialized_view(
    name="silver_tracking",
    comment="Estado más reciente y normalizado de tracking de envíos"
)
@dp.expect_all_or_drop({
    "tracking_valido": "tracking_id IS NOT NULL",
    "pedido_valido": "pedido_id IS NOT NULL",
    "fecha_valida": "fecha_actualizacion IS NOT NULL",
    "estado_valido":
        "estado_entrega IN ('EN_CAMINO','PENDIENTE','ENTREGADO','DEVUELTO')"
})
def silver_tracking():

    df = spark.read.table(
        "electrocasa.bronze.bronze_tracking"
    )

    estado = normalizar_codigo(
        F.col("estado_entrega")
    )

    df = (
        df

        .withColumn(
            "courier_raw",
            F.col("courier")
        )

        .withColumn(
            "courier",
            normalizar_codigo(
                F.col("courier")
            )
        )

        .withColumn(
            "estado_entrega_raw",
            F.col("estado_entrega")
        )

        .withColumn(
            "estado_entrega",
            F.when(
                estado.isin(
                    "EN_CAMINO",
                    "EN_TRANSITO"
                ),
                F.lit("EN_CAMINO")
            )
            .when(
                estado == "PENDIENTE",
                F.lit("PENDIENTE")
            )
            .when(
                estado == "ENTREGADO",
                F.lit("ENTREGADO")
            )
            .when(
                estado == "DEVUELTO",
                F.lit("DEVUELTO")
            )
            .otherwise(estado)
        )

        .withColumn(
            "fecha_actualizacion",
            F.to_date(
                "fecha_actualizacion"
            )
        )

        .withColumn(
            "_silver_timestamp",
            F.current_timestamp()
        )
    )

    ventana = (
        Window
        .partitionBy("tracking_id")
        .orderBy(
            F.col(
                "fecha_actualizacion"
            ).desc_nulls_last(),

            F.col(
                "_ingestion_timestamp"
            ).desc_nulls_last()
        )
    )

    return (
        df
        .withColumn(
            "_rn",
            F.row_number().over(ventana)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================
# 7. CUARENTENA - VENTAS
# ============================================================
#
# Conserva registros que no cumplen las reglas críticas
# de silver_ventas.
# ============================================================

@dp.materialized_view(
    name="quarantine_ventas",
    comment="Registros rechazados de ventas con motivo de rechazo"
)
def quarantine_ventas():

    df = spark.read.table(
        "electrocasa.bronze.bronze_ventas"
    )

    cantidad = F.col("cantidad").cast("int")
    monto_total = F.col("monto_total").cast("double")
    fecha_venta = F.to_date("fecha_venta")

    invalid_venta_id = F.col("venta_id").isNull()
    invalid_sucursal = F.col("sucursal_id").isNull()
    invalid_producto = F.col("producto_id").isNull()
    invalid_cantidad = (
        cantidad.isNull() |
        (cantidad <= 0)
    )
    invalid_monto = (
        monto_total.isNull() |
        (monto_total <= 0)
    )
    invalid_fecha = fecha_venta.isNull()

    df = (
        df
        .withColumn(
            "_q_cantidad",
            cantidad
        )
        .withColumn(
            "_q_monto_total",
            monto_total
        )
        .withColumn(
            "_q_fecha_venta",
            fecha_venta
        )
        .withColumn(
            "motivo_rechazo",
            F.concat_ws(
                " | ",
                F.when(
                    invalid_venta_id,
                    "venta_id_nulo"
                ),
                F.when(
                    invalid_sucursal,
                    "sucursal_id_nulo"
                ),
                F.when(
                    invalid_producto,
                    "producto_id_nulo"
                ),
                F.when(
                    invalid_cantidad,
                    "cantidad_no_positiva"
                ),
                F.when(
                    invalid_monto,
                    "monto_total_no_positivo"
                ),
                F.when(
                    invalid_fecha,
                    "fecha_venta_nula_o_invalida"
                )
            )
        )
        .withColumn(
            "fuente",
            F.lit("bronze_ventas")
        )
        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp()
        )
        .filter(
            invalid_venta_id |
            invalid_sucursal |
            invalid_producto |
            invalid_cantidad |
            invalid_monto |
            invalid_fecha
        )
        .drop(
            "_q_cantidad",
            "_q_monto_total",
            "_q_fecha_venta"
        )
    )

    return df


# ============================================================
# 8. CUARENTENA - PRODUCTOS
# ============================================================

@dp.materialized_view(
    name="quarantine_productos",
    comment="Registros rechazados de productos con motivo de rechazo"
)
def quarantine_productos():

    df = spark.read.table(
        "electrocasa.bronze.bronze_productos"
    )

    precio = (
        F.regexp_replace(
            F.col("precio_lista"),
            r"[^0-9.\-]",
            ""
        )
        .cast("double")
    )

    invalid_producto_id = F.col("producto_id").isNull()
    invalid_nombre = F.col("nombre_producto").isNull()
    invalid_categoria = F.col("categoria").isNull()
    invalid_precio = (
        precio.isNull() |
        (precio <= 0)
    )

    df = (
        df
        .withColumn(
            "_q_precio_lista",
            precio
        )
        .withColumn(
            "motivo_rechazo",
            F.concat_ws(
                " | ",
                F.when(
                    invalid_producto_id,
                    "producto_id_nulo"
                ),
                F.when(
                    invalid_nombre,
                    "nombre_producto_nulo"
                ),
                F.when(
                    invalid_categoria,
                    "categoria_nula"
                ),
                F.when(
                    invalid_precio,
                    "precio_lista_no_positivo"
                )
            )
        )
        .withColumn(
            "fuente",
            F.lit("bronze_productos")
        )
        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp()
        )
        .filter(
            invalid_producto_id |
            invalid_nombre |
            invalid_categoria |
            invalid_precio
        )
        .drop("_q_precio_lista")
    )

    return df


# ============================================================
# 9. CUARENTENA - EMPLEADOS
# ============================================================

@dp.materialized_view(
    name="quarantine_empleados",
    comment="Registros rechazados de empleados con motivo de rechazo"
)
def quarantine_empleados():

    df = spark.read.table(
        "electrocasa.bronze.bronze_empleados"
    )

    salario = F.col("salario").cast("double")
    fecha_evento = F.to_date("fecha_evento")
    tipo_evento = normalizar_codigo(
        F.col("tipo_evento")
    )

    invalid_empleado = F.col("id_empleado").isNull()
    invalid_nombre = F.col("nombre").isNull()
    invalid_sucursal = F.col("sucursal_id").isNull()
    invalid_salario = (
        salario.isNull() |
        (salario <= 0)
    )
    invalid_fecha = fecha_evento.isNull()
    invalid_tipo = (
        tipo_evento.isNull() |
        ~tipo_evento.isin(
            "ALTA",
            "BAJA",
            "TRANSFERENCIA",
            "CAMBIO_SALARIO"
        )
    )

    df = (
        df
        .withColumn(
            "_q_salario",
            salario
        )
        .withColumn(
            "_q_fecha_evento",
            fecha_evento
        )
        .withColumn(
            "_q_tipo_evento",
            tipo_evento
        )
        .withColumn(
            "motivo_rechazo",
            F.concat_ws(
                " | ",
                F.when(
                    invalid_empleado,
                    "id_empleado_nulo"
                ),
                F.when(
                    invalid_nombre,
                    "nombre_nulo"
                ),
                F.when(
                    invalid_sucursal,
                    "sucursal_id_nulo"
                ),
                F.when(
                    invalid_salario,
                    "salario_no_positivo"
                ),
                F.when(
                    invalid_fecha,
                    "fecha_evento_nula_o_invalida"
                ),
                F.when(
                    invalid_tipo,
                    "tipo_evento_invalido"
                )
            )
        )
        .withColumn(
            "fuente",
            F.lit("bronze_empleados")
        )
        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp()
        )
        .filter(
            invalid_empleado |
            invalid_nombre |
            invalid_sucursal |
            invalid_salario |
            invalid_fecha |
            invalid_tipo
        )
        .drop(
            "_q_salario",
            "_q_fecha_evento",
            "_q_tipo_evento"
        )
    )

    return df


# ============================================================
# 10. CUARENTENA - RESENAS
# ============================================================

@dp.materialized_view(
    name="quarantine_resenas",
    comment="Registros rechazados de reseñas con motivo de rechazo"
)
def quarantine_resenas():

    df = spark.read.table(
        "electrocasa.bronze.bronze_resenas"
    )

    calificacion = F.col(
        "calificacion"
    ).cast("int")

    fecha_resena = F.to_date(
        "fecha_resena"
    )

    invalid_resena = F.col(
        "resena_id"
    ).isNull()

    invalid_producto = F.col(
        "producto_id"
    ).isNull()

    invalid_cliente = F.col(
        "cliente_id"
    ).isNull()

    invalid_calificacion = (
        calificacion.isNull() |
        (calificacion < 1) |
        (calificacion > 5)
    )

    invalid_fecha = fecha_resena.isNull()

    df = (
        df
        .withColumn(
            "_q_calificacion",
            calificacion
        )
        .withColumn(
            "_q_fecha_resena",
            fecha_resena
        )
        .withColumn(
            "motivo_rechazo",
            F.concat_ws(
                " | ",
                F.when(
                    invalid_resena,
                    "resena_id_nulo"
                ),
                F.when(
                    invalid_producto,
                    "producto_id_nulo"
                ),
                F.when(
                    invalid_cliente,
                    "cliente_id_nulo"
                ),
                F.when(
                    invalid_calificacion,
                    "calificacion_fuera_de_rango_1_5"
                ),
                F.when(
                    invalid_fecha,
                    "fecha_resena_nula_o_invalida"
                )
            )
        )
        .withColumn(
            "fuente",
            F.lit("bronze_resenas")
        )
        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp()
        )
        .filter(
            invalid_resena |
            invalid_producto |
            invalid_cliente |
            invalid_calificacion |
            invalid_fecha
        )
        .drop(
            "_q_calificacion",
            "_q_fecha_resena"
        )
    )

    return df


# ============================================================
# 11. CUARENTENA - DEVOLUCIONES
# ============================================================

@dp.materialized_view(
    name="quarantine_devoluciones",
    comment="Registros rechazados de devoluciones con motivo de rechazo"
)
def quarantine_devoluciones():

    df = spark.read.table(
        "electrocasa.bronze.bronze_devoluciones"
    )

    monto_reembolso = F.col(
        "monto_reembolso"
    ).cast("double")

    fecha_devolucion = F.to_date(
        "fecha_devolucion"
    )

    motivo = normalizar_codigo(
        F.col("motivo")
    )

    invalid_devolucion = F.col(
        "devolucion_id"
    ).isNull()

    invalid_pedido = F.col(
        "pedido_id"
    ).isNull()

    invalid_producto = F.col(
        "producto_id"
    ).isNull()

    invalid_sucursal = F.col(
        "sucursal_id"
    ).isNull()

    invalid_motivo = motivo.isNull()

    invalid_reembolso = (
        monto_reembolso.isNull() |
        (monto_reembolso <= 0)
    )

    invalid_fecha = fecha_devolucion.isNull()

    df = (
        df
        .withColumn(
            "_q_monto_reembolso",
            monto_reembolso
        )
        .withColumn(
            "_q_fecha_devolucion",
            fecha_devolucion
        )
        .withColumn(
            "_q_motivo",
            motivo
        )
        .withColumn(
            "motivo_rechazo",
            F.concat_ws(
                " | ",
                F.when(
                    invalid_devolucion,
                    "devolucion_id_nulo"
                ),
                F.when(
                    invalid_pedido,
                    "pedido_id_nulo"
                ),
                F.when(
                    invalid_producto,
                    "producto_id_nulo"
                ),
                F.when(
                    invalid_sucursal,
                    "sucursal_id_nulo"
                ),
                F.when(
                    invalid_motivo,
                    "motivo_nulo"
                ),
                F.when(
                    invalid_reembolso,
                    "monto_reembolso_no_positivo"
                ),
                F.when(
                    invalid_fecha,
                    "fecha_devolucion_nula_o_invalida"
                )
            )
        )
        .withColumn(
            "fuente",
            F.lit("bronze_devoluciones")
        )
        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp()
        )
        .filter(
            invalid_devolucion |
            invalid_pedido |
            invalid_producto |
            invalid_sucursal |
            invalid_motivo |
            invalid_reembolso |
            invalid_fecha
        )
        .drop(
            "_q_monto_reembolso",
            "_q_fecha_devolucion",
            "_q_motivo"
        )
    )

    return df


# ============================================================
# 12. CUARENTENA - TRACKING
# ============================================================

@dp.materialized_view(
    name="quarantine_tracking",
    comment="Registros rechazados de tracking con motivo de rechazo"
)
def quarantine_tracking():

    df = spark.read.table(
        "electrocasa.bronze.bronze_tracking"
    )

    estado = normalizar_codigo(
        F.col("estado_entrega")
    )

    fecha_actualizacion = F.to_date(
        F.col("fecha_actualizacion")
    )

    estado_normalizado = (
        F.when(
            estado.isin(
                "EN_CAMINO",
                "EN_TRANSITO"
            ),
            F.lit("EN_CAMINO")
        )
        .when(
            estado == "PENDIENTE",
            F.lit("PENDIENTE")
        )
        .when(
            estado == "ENTREGADO",
            F.lit("ENTREGADO")
        )
        .when(
            estado == "DEVUELTO",
            F.lit("DEVUELTO")
        )
        .otherwise(estado)
    )

    invalid_tracking = F.col(
        "tracking_id"
    ).isNull()

    invalid_pedido = F.col(
        "pedido_id"
    ).isNull()

    invalid_fecha = fecha_actualizacion.isNull()

    invalid_estado = (
        estado_normalizado.isNull() |
        ~estado_normalizado.isin(
            "EN_CAMINO",
            "PENDIENTE",
            "ENTREGADO",
            "DEVUELTO"
        )
    )

    df = (
        df
        .withColumn(
            "_q_estado_entrega",
            estado_normalizado
        )
        .withColumn(
            "_q_fecha_actualizacion",
            fecha_actualizacion
        )
        .withColumn(
            "motivo_rechazo",
            F.concat_ws(
                " | ",
                F.when(
                    invalid_tracking,
                    "tracking_id_nulo"
                ),
                F.when(
                    invalid_pedido,
                    "pedido_id_nulo"
                ),
                F.when(
                    invalid_fecha,
                    "fecha_actualizacion_nula_o_invalida"
                ),
                F.when(
                    invalid_estado,
                    "estado_entrega_invalido"
                )
            )
        )
        .withColumn(
            "fuente",
            F.lit("bronze_tracking")
        )
        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp()
        )
        .filter(
            invalid_tracking |
            invalid_pedido |
            invalid_fecha |
            invalid_estado
        )
        .drop(
            "_q_estado_entrega",
            "_q_fecha_actualizacion"
        )
    )

    return df