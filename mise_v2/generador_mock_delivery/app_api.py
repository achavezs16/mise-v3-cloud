"""Proyecto Generador de Mock y API de Ingesta para Plataformas de Delivery.

Actúa como el sistema emisor de pedidos (Uber Eats, Rappi, PedidosYa):
1. Expone la API REST que recibiría los webhooks de las plataformas externas.
2. Genera datos mock realistas (menú, clientes, precios CLP, propinas).
3. Ejecuta el traspaso masivo de alta velocidad hacia Amazon Kinesis Data Streams usando PutRecords.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import random
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("delivery.api")

# Configuración del Stream Kinesis en AWS
REGION_AWS = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
NOMBRE_STREAM = os.environ.get("STREAM_DELIVERY_NOMBRE", "pos-delivery-stream")

# --- Modelos de Dominio y Generador Mock ---

_ALFABETO = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generar_ulid() -> str:
    timestamp_ms = time.time_ns() // 1_000_000
    tiempo_chars = "".join(_ALFABETO[(timestamp_ms >> (5 * i)) & 31] for i in reversed(range(10)))
    random_chars = "".join(_ALFABETO[(secrets.randbits(80) >> (5 * i)) & 31] for i in reversed(range(16)))
    return tiempo_chars + random_chars


PLATAFORMAS = ["ubereats", "rappi", "pedidosya"]

CLIENTES = [
    ("Francisca Morales", "+56987654321", "Av. Providencia 1234, Depto 502, Providencia"),
    ("Sebastián Valenzuela", "+56976543210", "Los Leones 234, Providencia"),
    ("Valentina Silva", "+56965432109", "Av. Apoquindo 4500, Las Condes"),
    ("Diego Muñoz", "+56954321098", "Manuel Montt 890, Ñuñoa"),
    ("Camila Rojas", "+56943210987", "Irarrázaval 3200, Ñuñoa"),
    ("Matías Contreras", "+56932109876", "Av. Vitacura 5600, Vitacura"),
    ("Nicolás Castro", "+56910987654", "General Holley 145, Providencia"),
]

PRODUCTOS = [
    ("BURGER_CLASICA", "Hamburguesa Clásica", 7900),
    ("BURGER_DOBLE", "Hamburguesa Doble Queso Tocino", 9900),
    ("BURGER_VEGGIE", "Hamburguesa NotBurger Palta", 8900),
    ("PIZZA_PEPPERONI", "Pizza Pepperoni Individual", 8500),
    ("PIZZA_MARGARITA", "Pizza Margarita", 7500),
    ("PAPAS_RUSTICAS", "Papas Rústicas Cheddar", 4500),
    ("BEBIDA_COCA_ZERO", "Coca-Cola Zero 350ml", 1800),
    ("CERVEZA_IPA", "Cerveza Artesanal IPA 473ml", 3900),
]


def generar_pedido_mock(tenant_id: str = "demo", local_id: str = "local_01", plataforma: str | None = None) -> dict[str, Any]:
    plat = plataforma if plataforma in PLATAFORMAS else random.choice(PLATAFORMAS)
    cliente = random.choice(CLIENTES)
    prods = random.sample(PRODUCTOS, k=random.randint(1, 3))

    items = []
    subtotal = 0
    for prod_id, nombre, precio in prods:
        cant = random.randint(1, 2)
        st = precio * cant
        subtotal += st
        items.append({
            "item_id": generar_ulid(),
            "producto_id": prod_id,
            "nombre": nombre,
            "precio_unitario": precio,
            "cantidad": cant,
            "subtotal": st,
            "notas": "Sin cebolla" if random.random() > 0.6 else "",
            "estado": "en_espera",
        })

    propina = int(subtotal * 0.10) if random.random() > 0.4 else 0
    total = subtotal + propina
    pedido_id = generar_ulid()

    return {
        "pedido_id": pedido_id,
        "command_id": generar_ulid(),
        "event_id": generar_ulid(),
        "tenant_id": tenant_id,
        "local_id": local_id,
        "plataforma": plat,
        "orden_externa_id": f"{plat.upper()}-{random.randint(100000, 999999)}",
        "tipo_pedido": "delivery",
        "recibido_en": datetime.now(timezone.utc).isoformat(),
        "cliente": {"nombre": cliente[0], "telefono": cliente[1], "direccion": cliente[2]},
        "repartidor": {"nombre": "Repartidor Asignado", "vehiculo": "Moto"},
        "items": items,
        "subtotal": subtotal,
        "propina": propina,
        "total": total,
        "metodo_pago": "online_platform",
        "estado": "abierto",
    }


# --- Cliente Kinesis con Autodetección ---

class TransmisorKinesis:
    def __init__(self, region: str = REGION_AWS, stream_name: str = NOMBRE_STREAM) -> None:
        self.region = region
        self.stream_name = stream_name
        self.es_real = False
        self.client = None

        try:
            session = boto3.Session(region_name=self.region)
            sts = session.client("sts")
            sts.get_caller_identity()
            self.client = session.client("kinesis")
            self.es_real = True
            log.info(f"Conectado a AWS Kinesis real en {self.region}")
        except Exception as e:
            log.warning(f"No hay credenciales activas de AWS ({e}). Usando modo emulación.")
            self.es_real = False

    def enviar_lote_kinesis(self, pedidos: list[dict[str, Any]]) -> dict[str, Any]:
        """Envía lotes de hasta 500 registros usando PutRecords."""
        if not pedidos:
            return {"total": 0, "exitosos": 0, "fallidos": 0}

        total_exitosos = 0
        total_fallidos = 0
        t0 = time.monotonic()
        shards_usados: dict[str, int] = {}

        # Particionar en bloques de 500
        for i in range(0, len(pedidos), 500):
            sub_lote = pedidos[i : i + 500]
            records = []
            for p in sub_lote:
                p_key = f"{p['tenant_id']}#{p['local_id']}"
                payload = json.dumps(p, ensure_ascii=False).encode("utf-8")
                records.append({"Data": payload, "PartitionKey": p_key})

            if self.es_real:
                res = self.client.put_records(StreamName=self.stream_name, Records=records)
                fallidos = res.get("FailedRecordCount", 0)
                total_fallidos += fallidos
                total_exitosos += len(sub_lote) - fallidos
                for r in res.get("Records", []):
                    s_id = r.get("ShardId", "shardId-000000000000")
                    shards_usados[s_id] = shards_usados.get(s_id, 0) + 1
            else:
                # Simulado local
                total_exitosos += len(sub_lote)
                shards_usados["shardId-000000000000"] = len(sub_lote)

        duracion_ms = int((time.monotonic() - t0) * 1000)
        tasa = round(total_exitosos / (duracion_ms / 1000.0), 2) if duracion_ms > 0 else total_exitosos

        return {
            "stream": self.stream_name,
            "total_pedidos": len(pedidos),
            "exitosos": total_exitosos,
            "fallidos": total_fallidos,
            "tiempo_ms": duracion_ms,
            "pedidos_por_segundo": tasa,
            "distribucion_shards": shards_usados,
            "modo": "AWS_REAL" if self.es_real else "EMULADO_LOCAL",
        }


transmisor = TransmisorKinesis()

# --- API FastAPI ---

app = FastAPI(
    title="API Ingesta de Delivery & Traspaso Masivo a Kinesis",
    description="Proyecto Emisor: Simula plataformas de delivery y transfiere masivamente hacia AWS Kinesis.",
    version="1.0.0",
)


class TraspasoMasivoSchema(BaseModel):
    cantidad: int = Field(default=50, ge=1, le=2000, description="Cantidad de pedidos a transferir")
    tenant_id: str = "demo"
    local_id: str = "local_01"
    plataforma: Optional[str] = Field(default=None, description="ubereats, rappi, pedidosya o null para todas")


@app.get("/")
def inicio():
    return {
        "proyecto": "Generador Mock & API Delivery",
        "destino_kinesis": NOMBRE_STREAM,
        "region_aws": REGION_AWS,
        "modo_transmisor": "AWS_REAL" if transmisor.es_real else "EMULADO_LOCAL",
        "swagger_docs": "/docs",
    }


@app.post("/api/delivery/traspaso-masivo")
def ejecutar_traspaso_masivo(body: TraspasoMasivoSchema):
    """Genera N pedidos de delivery y los transfiere en ráfaga a Amazon Kinesis."""
    log.info(f"Iniciando traspaso masivo de {body.cantidad} pedidos...")

    # 1. Generar datos mock
    pedidos = [
        generar_pedido_mock(tenant_id=body.tenant_id, local_id=body.local_id, plataforma=body.plataforma)
        for _ in range(body.cantidad)
    ]

    # 2. Transferir a Kinesis
    res = transmisor.enviar_lote_kinesis(pedidos)

    return {
        "mensaje": f"Traspaso masivo de {body.cantidad} pedidos completado exitosamente hacia Kinesis.",
        "resultado_kinesis": res,
        "ejemplo_primer_pedido": pedidos[0]["orden_externa_id"],
        "monto_total_transferido_clp": sum(p["total"] for p in pedidos),
    }


@app.get("/api/delivery/estado-stream")
def estado_stream():
    """Consulta el estado del stream de Kinesis en AWS."""
    if transmisor.es_real:
        try:
            desc = transmisor.client.describe_stream_summary(StreamName=NOMBRE_STREAM)["StreamDescriptionSummary"]
            return {
                "StreamName": desc["StreamName"],
                "StreamARN": desc["StreamARN"],
                "StreamStatus": desc["StreamStatus"],
                "OpenShardCount": desc["OpenShardCount"],
                "RetentionPeriodHours": desc["RetentionPeriodHours"],
                "Modo": "AWS_REAL",
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    else:
        return {
            "StreamName": NOMBRE_STREAM,
            "StreamStatus": "ACTIVE (EMULADO)",
            "OpenShardCount": 2,
            "Modo": "EMULADO_LOCAL",
        }
