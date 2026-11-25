"""Función Lambda `consumidor-delivery`: procesa registros de Amazon Kinesis Data Streams.

Disparador: Stream `pos-delivery-stream` con EventSourceMapping de AWS Lambda.
Variables de entorno requeridas:
    - TABLA_OPERACION
    - TABLA_CONFIGURACION
    - LOG_LEVEL (opcional)

Respuestas:
    Devuelve `batchItemFailures` para soportar ReportBatchItemFailures en AWS Lambda.
    Los registros exitosos se marcan como procesados; si un registro falla, solo ese
    registro (y los posteriores) se reintentan según la política de bisect.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

log = logging.getLogger("pos.consumidor_delivery")
log.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

TABLA_OPERACION = os.environ.get("TABLA_OPERACION", "operacion")
serializer = TypeSerializer()


def serializar(item: dict[str, Any]) -> dict[str, Any]:
    return {k: serializer.serialize(v) for k, v in item.items()}


def procesar_registro_kinesis(datos_pedido: dict[str, Any], dynamodb_client) -> None:
    """Aplica la transacción atómica en DynamoDB (tabla operacion) para un pedido de delivery."""
    tenant_id = datos_pedido["tenant_id"]
    local_id = datos_pedido["local_id"]
    plataforma = datos_pedido["plataforma"].upper()
    orden_externa_id = datos_pedido["orden_externa_id"]
    pedido_id = datos_pedido["pedido_id"]
    command_id = datos_pedido.get("command_id", f"CMD-{pedido_id}")
    event_id = datos_pedido.get("event_id", f"EVT-{pedido_id}")
    L = f"TENANT#{tenant_id}#LOCAL#{local_id}"
    ahora_iso = datetime.now(timezone.utc).isoformat()

    pk_ped = f"{L}#PED#{pedido_id}"
    pk_cmd = f"{L}#CMD#{command_id}"
    pk_ext = f"{L}#EXTERNO#{plataforma}"
    pk_evt = f"{L}#EVT"
    pk_imp = f"{L}#IMP#cocina"

    transact_items = []

    # 1. Comando idempotente
    transact_items.append({
        "Put": {
            "TableName": TABLA_OPERACION,
            "Item": serializar({
                "PK": pk_cmd,
                "SK": "CMD",
                "tipo_item": "CMD",
                "estado": "aplicado",
                "http_status": 200,
                "creado_en": ahora_iso,
                "expira_en": int(datetime.now(timezone.utc).timestamp()) + (48 * 3600),
            }),
            "ConditionExpression": "attribute_not_exists(PK)",
        }
    })

    # 2. Índice de orden externa
    transact_items.append({
        "Put": {
            "TableName": TABLA_OPERACION,
            "Item": serializar({
                "PK": pk_ext,
                "SK": f"ORDEN#{orden_externa_id}",
                "tipo_item": "EXTERNO_INDEX",
                "plataforma": plataforma.lower(),
                "orden_externa_id": orden_externa_id,
                "pedido_id": pedido_id,
                "total": int(datos_pedido["total"]),
                "recibido_en": datos_pedido.get("recibido_en", ahora_iso),
            }),
            "ConditionExpression": "attribute_not_exists(PK)",
        }
    })

    # 3. Cabecera del pedido (PEDIDO)
    transact_items.append({
        "Put": {
            "TableName": TABLA_OPERACION,
            "Item": serializar({
                "PK": pk_ped,
                "SK": "PEDIDO",
                "tipo_item": "PEDIDO",
                "tipo_pedido": "delivery",
                "plataforma": plataforma.lower(),
                "orden_externa_id": orden_externa_id,
                "estado": "abierto",
                "version": 1,
                "subtotal": int(datos_pedido["subtotal"]),
                "propina": int(datos_pedido.get("propina", 0)),
                "total": int(datos_pedido["total"]),
                "cliente": datos_pedido.get("cliente", {}),
                "repartidor": datos_pedido.get("repartidor", {}),
                "recibido_en": datos_pedido.get("recibido_en", ahora_iso),
                "origen": "kinesis.stream",
            }),
            "ConditionExpression": "attribute_not_exists(PK)",
        }
    })

    # 4. Ítems del pedido (máximo hasta completar el límite de 100 de DynamoDB)
    for it in datos_pedido.get("items", [])[:90]:
        item_id = it.get("item_id", f"IT-{ids_unico()}")
        transact_items.append({
            "Put": {
                "TableName": TABLA_OPERACION,
                "Item": serializar({
                    "PK": pk_ped,
                    "SK": f"ITEM#{item_id}",
                    "tipo_item": "ITEM",
                    "producto_id": it["producto_id"],
                    "nombre": it["nombre"],
                    "precio_unitario": int(it["precio_unitario"]),
                    "cantidad": int(it["cantidad"]),
                    "subtotal": int(it["subtotal"]),
                    "notas": it.get("notas", ""),
                    "estado": "en_espera",
                }),
            }
        })

    # 5. Evento Outbox para auditoría y analítica
    transact_items.append({
        "Put": {
            "TableName": TABLA_OPERACION,
            "Item": serializar({
                "PK": pk_evt,
                "SK": f"EVT#{event_id}",
                "tipo_item": "EVT",
                "tipo": "delivery.ingresado",
                "tenant_id": tenant_id,
                "local_id": local_id,
                "ocurrido_en": datos_pedido.get("recibido_en", ahora_iso),
                "datos": {
                    "pedido_id": pedido_id,
                    "plataforma": plataforma.lower(),
                    "orden_externa_id": orden_externa_id,
                    "total": int(datos_pedido["total"]),
                },
            }),
        }
    })

    # 6. Ticket para la impresora de cocina
    trabajo_id = f"TRAB-{pedido_id[:12]}"
    transact_items.append({
        "Put": {
            "TableName": TABLA_OPERACION,
            "Item": serializar({
                "PK": pk_imp,
                "SK": f"PEND#{trabajo_id}",
                "tipo_item": "PENDIENTE_IMPRESION",
                "tipo_ticket": "comanda_delivery",
                "pedido_id": pedido_id,
                "plataforma": plataforma.lower(),
                "creado_en": ahora_iso,
            }),
        }
    })

    try:
        dynamodb_client.transact_write_items(TransactItems=transact_items)
        log.info(f"Pedido delivery {pedido_id} ({plataforma}) guardado en DynamoDB.")
    except ClientError as e:
        codigo = e.response.get("Error", {}).get("Code")
        if codigo == "TransactionCanceledException":
            # Si falló la condición del CMD o de la orden externa, ya existe (idempotencia)
            log.warning(f"Transacción ya aplicada o duplicada para {pedido_id}: {e}")
        else:
            raise e


def ids_unico() -> str:
    import time
    return f"{int(time.time()*1000)}"


def lambda_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Manejador principal de eventos Kinesis para AWS Lambda."""
    dynamodb = boto3.client("dynamodb")
    fallos = []

    registros = event.get("Records", [])
    log.info(f"Procesando lote de {len(registros)} registros de Kinesis...")

    for r in registros:
        kinesis_data = r.get("kinesis", {})
        seq_num = kinesis_data.get("sequenceNumber", "")
        raw_b64 = kinesis_data.get("data", "")

        try:
            cuerpo_bytes = base64.b64decode(raw_b64)
            datos_pedido = json.loads(cuerpo_bytes.decode("utf-8"))
            procesar_registro_kinesis(datos_pedido, dynamodb)
        except Exception as e:
            log.error(f"Error procesando registro Kinesis seq {seq_num}: {e}", exc_info=True)
            fallos.append({"itemIdentifier": seq_num})

    return {"batchItemFailures": fallos}
