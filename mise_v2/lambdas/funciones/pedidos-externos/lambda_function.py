"""Función `pedidos-externos`: webhook de plataformas de delivery e integración con Kinesis.

Recibe pedidos de delivery de plataformas (Uber Eats, Rappi, PedidosYa):
1. Verifica la firma HMAC-SHA256 con SECRETO_WEBHOOK en tiempo constante.
2. Si STREAM_DELIVERY_ACTIVO está activo, publica el pedido en Amazon Kinesis Data Streams.
3. Si no está activo (compatibilidad v2), responde 501 NO_IMPLEMENTADO.

Ruta adicional:
- POST /locales/{local_id}/delivery/importar  → importa N pedidos mock desde apps de delivery vía Kinesis
- GET  /locales/{local_id}/delivery/importar  → estado del stream de delivery

Manejador: lambda_function.lambda_handler
Variables: SECRETO_WEBHOOK, TABLA_CONFIGURACION, TABLA_OPERACION, STREAM_DELIVERY_NOMBRE (opcional),
           STREAM_DELIVERY_ACTIVO (opcional)
"""

import hashlib
import hmac
import json
import os
import random
import time
from datetime import datetime, timezone

from pos_dominio import claves, ids
from pos_dominio.errores import ErrorDominio, NoAutenticado
from pos_dominio.infra import api, aws, http

aws.exigir_entorno("SECRETO_WEBHOOK", "TABLA_OPERACION", "TABLA_CONFIGURACION")
app = http.Aplicacion("pedidos-externos")

STREAM_DELIVERY = os.environ.get("STREAM_DELIVERY_NOMBRE", "pos-delivery-stream")
STREAM_ACTIVO = os.environ.get("STREAM_DELIVERY_ACTIVO", "0") == "1"

PLATAFORMAS = ["UBEREATS", "RAPPI", "PEDIDOSYA"]
NOMBRES = ["Martina López", "Sebastián Rojas", "Valentina García", "Diego Morales",
           "Camila Fernández", "Nicolás Castro", "Javiera Muñoz", "Tomás Pérez",
           "Isidora Silva", "Matías González", "Sofía Reyes", "Benjamín Vargas"]
CALLES = ["Av. Providencia 1234", "Loreto 456, Depto 3B", "Manuel Montt 789",
          "Av. Italia 321, Piso 2", "Pedro de Valdivia 654", "Condell 987, Depto 12",
          "Av. Irarrázaval 111", "Ricardo Lyon 222, Depto 5A"]
ITEMS_MENU = [
    {"nombre": "Hamburguesa Clásica", "precio": 8990},
    {"nombre": "Pizza Margherita", "precio": 11990},
    {"nombre": "Combo Pollo BBQ", "precio": 9490},
    {"nombre": "Papas Fritas Grandes", "precio": 3490},
    {"nombre": "Bebida 500ml", "precio": 1990},
    {"nombre": "Ensalada César", "precio": 7490},
    {"nombre": "Brownie con Helado", "precio": 4990},
    {"nombre": "Jugo Natural", "precio": 2990},
]


def _generar_pedido_mock(tenant_id: str, local_id: str) -> dict:
    """Genera un pedido de delivery realista para cualquier plataforma."""
    plataforma = random.choice(PLATAFORMAS)
    n_items = random.randint(1, 4)
    items = [dict(item) for item in random.sample(ITEMS_MENU, n_items)]
    for item in items:
        item["item_id"] = ids.nuevo_ulid()
        item["producto_id"] = f"MOCK-{ITEMS_MENU.index({"nombre": item["nombre"], "precio": item["precio"]}) + 1}"
        item["precio_unitario"] = item["precio"]
        item["cantidad"] = random.randint(1, 3)
        item["subtotal"] = item["precio"] * item["cantidad"]
    total = sum(i["subtotal"] for i in items)
    orden_id = f"{plataforma}-{random.randint(100000, 999999)}"
    return {
        "pedido_id": ids.nuevo_ulid(),
        "command_id": ids.nuevo_ulid(),
        "event_id": ids.nuevo_ulid(),
        "tenant_id": tenant_id,
        "local_id": local_id,
        "plataforma": plataforma,
        "orden_externa_id": orden_id,
        "cliente": {"nombre": random.choice(NOMBRES), "telefono": f"+569{random.randint(10000000,99999999)}"},
        "direccion_despacho": random.choice(CALLES),
        "items": items,
        "subtotal": total,
        "propina": 0,
        "total": total,
        "recibido_en": datetime.now(timezone.utc).isoformat(),
        "estado": "PENDIENTE",
    }


class NoImplementado(ErrorDominio):
    http = 501
    codigo = "NO_IMPLEMENTADO"


def firma_valida(cuerpo: bytes, firma: str | None, secreto: str) -> bool:
    if not firma:
        return False
    esperada = hmac.new(secreto.encode("utf-8"), cuerpo, hashlib.sha256).hexdigest()
    return hmac.compare_digest(esperada, firma.strip().lower())


@app.ruta("POST /webhooks/{plataforma}/{tenant_id}/{local_id}", publica=True)
def recibir(peticion):
    if not firma_valida(peticion.cuerpo_bytes, peticion.encabezados.get("x-firma"), os.environ["SECRETO_WEBHOOK"]):
        raise NoAutenticado("Firma inválida", codigo="FIRMA_INVALIDA")

    tenant_id = peticion.parametros.get("tenant_id")
    local_id = peticion.parametros.get("local_id")
    plataforma = peticion.parametros.get("plataforma")

    claves.local(tenant_id, local_id)
    claves.validar_id(plataforma, "plataforma")

    if not STREAM_ACTIVO:
        raise NoImplementado("La integración con plataformas de delivery aún no está disponible")

    # Inyección en Amazon Kinesis Data Streams
    cuerpo = peticion.json()
    cuerpo["tenant_id"] = tenant_id
    cuerpo["local_id"] = local_id
    cuerpo["plataforma"] = plataforma
    cuerpo["recibido_en"] = cuerpo.get("recibido_en") or datetime.now(timezone.utc).isoformat()
    if not cuerpo.get("pedido_id"):
        cuerpo["pedido_id"] = ids.nuevo_ulid()
    if not cuerpo.get("command_id"):
        cuerpo["command_id"] = peticion.encabezados.get("x-command-id") or ids.nuevo_ulid()
    if not cuerpo.get("event_id"):
        cuerpo["event_id"] = ids.nuevo_ulid()

    partition_key = f"{tenant_id}#{local_id}"
    aws.cliente("kinesis").put_record(
        StreamName=STREAM_DELIVERY,
        Data=json.dumps(cuerpo, ensure_ascii=False).encode("utf-8"),
        PartitionKey=partition_key,
    )

    return 202, {
        "resultado": "ENVIADO_A_STREAM",
        "pedido_id": cuerpo["pedido_id"],
        "stream": STREAM_DELIVERY,
        "partition_key": partition_key,
    }


@app.ruta("POST /locales/{local_id}/delivery/importar")
def importar_delivery(peticion):
    """Importa pedidos mock de apps de delivery hacia Kinesis (flujo masivo desde el panel admin)."""
    ctx = api.contexto(peticion, accion="admin.estructura")
    local_id = ctx.local_id
    tenant_id = ctx.tenant_id

    cuerpo = peticion.json()
    cantidad = max(1, min(int(cuerpo.get("cantidad", 20)), 500))

    kinesis = aws.cliente("kinesis")

    # Verificar que el stream existe
    try:
        info = kinesis.describe_stream_summary(StreamName=STREAM_DELIVERY)
        estado = info["StreamDescriptionSummary"]["StreamStatus"]
        if estado != "ACTIVE":
            raise ErrorDominio(f"Stream '{STREAM_DELIVERY}' no está ACTIVE (estado: {estado})")
    except kinesis.exceptions.ResourceNotFoundException:
        raise ErrorDominio(
            f"Stream '{STREAM_DELIVERY}' no encontrado. Ejecuta 'bash infra/kinesis/desplegar_kinesis.sh' en CloudShell."
        )

    # Enviar en lotes de 25 registros
    LOTE = 25
    enviados = 0
    fallidos = 0
    pedidos_enviados = []
    t0 = time.time()

    for i in range(0, cantidad, LOTE):
        pedidos_lote = [_generar_pedido_mock(tenant_id, local_id) for _ in range(min(LOTE, cantidad - i))]
        registros = [
            {
                "Data": json.dumps(p, ensure_ascii=False).encode("utf-8"),
                "PartitionKey": f"{tenant_id}#{local_id}",
            }
            for p in pedidos_lote
        ]
        respuesta = kinesis.put_records(StreamName=STREAM_DELIVERY, Records=registros)
        enviados += len(registros) - respuesta.get("FailedRecordCount", 0)
        fallidos += respuesta.get("FailedRecordCount", 0)
        for pedido, registro in zip(pedidos_lote, respuesta["Records"]):
            if not registro.get("ErrorCode"):
                pedidos_enviados.append(pedido["pedido_id"])

    ms = int((time.time() - t0) * 1000)
    return 200, {
        "resultado": "IMPORTACION_COMPLETADA",
        "stream": STREAM_DELIVERY,
        "cantidad_solicitada": cantidad,
        "pedido_ids": pedidos_enviados,
        "enviados": enviados,
        "fallidos": fallidos,
        "duracion_ms": ms,
        "plataformas": PLATAFORMAS,
        "mensaje": f"Se importaron {enviados} pedidos de delivery a Kinesis en {ms} ms.",
    }


@app.ruta("GET /locales/{local_id}/delivery/importar")
def estado_delivery(peticion):
    """Devuelve el estado actual del stream de delivery."""
    api.contexto(peticion, accion="admin.estructura")
    kinesis = aws.cliente("kinesis")
    try:
        info = kinesis.describe_stream_summary(StreamName=STREAM_DELIVERY)
        s = info["StreamDescriptionSummary"]
        return 200, {
            "stream": STREAM_DELIVERY,
            "estado": s["StreamStatus"],
            "shards": s["OpenShardCount"],
            "retencion_horas": s["RetentionPeriodHours"],
            "arn": s["StreamARN"],
            "activo": s["StreamStatus"] == "ACTIVE",
        }
    except kinesis.exceptions.ResourceNotFoundException:
        return 200, {
            "stream": STREAM_DELIVERY,
            "estado": "NO_EXISTE",
            "activo": False,
            "mensaje": "El stream de delivery no ha sido creado. Ejecuta desplegar_kinesis.sh en CloudShell.",
        }



@app.ruta("POST /locales/{local_id}/delivery/consultar")
def consultar_delivery(peticion):
    """Confirma la persistencia de pedidos enviados a Kinesis."""
    ctx = api.contexto(peticion, accion="admin.estructura")
    L = claves.local(ctx.tenant_id, ctx.local_id)

    cuerpo = peticion.json()
    pedido_ids = cuerpo.get("pedido_ids", [])

    if not isinstance(pedido_ids, list) or len(pedido_ids) > 500:
        raise ErrorDominio("Se permiten hasta 500 identificadores")

    pedido_ids = list(dict.fromkeys(
        claves.validar_id(p, "pedido_id") for p in pedido_ids
    ))

    tabla = os.environ["TABLA_OPERACION"]
    dynamo = aws.cliente("dynamodb")
    pedidos = []
    sin_confirmar = 0

    for i in range(0, len(pedido_ids), 100):
        claves_pedidos = [
            {
                "PK": {"S": claves.pedido(L, pedido_id)},
                "SK": {"S": claves.SK_PEDIDO},
            }
            for pedido_id in pedido_ids[i:i + 100]
        ]

        respuesta = dynamo.batch_get_item(
            RequestItems={
                tabla: {
                    "Keys": claves_pedidos,
                    "ConsistentRead": True,
                    "ProjectionExpression":
                        "#pid, #plat, #total, #items, #fecha, #origen",
                    "ExpressionAttributeNames": {
                        "#pid": "pedido_id",
                        "#plat": "plataforma",
                        "#total": "total",
                        "#items": "n_items",
                        "#fecha": "recibido_en",
                        "#origen": "origen",
                    },
                }
            }
        )

        sin_confirmar += len(
            respuesta.get("UnprocessedKeys", {})
            .get(tabla, {}).get("Keys", [])
        )

        for item in respuesta.get("Responses", {}).get(tabla, []):
            if item.get("origen", {}).get("S") != "kinesis.stream":
                continue

            pedidos.append({
                "pedido_id": item["pedido_id"]["S"],
                "plataforma": item["plataforma"]["S"],
                "total": int(item["total"]["N"]),
                "n_items": int(item["n_items"]["N"]),
                "recibido_en": item["recibido_en"]["S"],
                "estado": "persistido",
            })

    pedidos.sort(key=lambda p: p["recibido_en"], reverse=True)

    return 200, {
        "consultados": len(pedido_ids),
        "persistidos": len(pedidos),
        "pendientes": len(pedido_ids) - len(pedidos),
        "claves_sin_procesar": sin_confirmar,
        "pedidos": pedidos,
    }


lambda_handler = app
