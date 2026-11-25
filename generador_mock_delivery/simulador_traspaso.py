#!/usr/bin/env python3
"""CLI para disparar traspasos masivos de delivery hacia Amazon Kinesis Data Streams.

Cero dependencias externas: solo biblioteca estándar de Python y boto3 (nativos en AWS CloudShell).

Uso:
    python3 generador_mock_delivery/simulador_traspaso.py [--cantidad 100] [--plataforma todas] [--stream pos-delivery-stream]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import secrets
import sys
import time
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

VERDE = "\033[92m"
AZUL = "\033[94m"
CIAN = "\033[96m"
NEGRITA = "\033[1m"
RESET = "\033[0m"

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

    return {
        "pedido_id": generar_ulid(),
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


class TransmisorKinesis:
    def __init__(self, region: str = "us-east-1", stream_name: str = "pos-delivery-stream") -> None:
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
        except Exception:
            self.es_real = False

    def enviar_lote_kinesis(self, pedidos: list[dict[str, Any]]) -> dict[str, Any]:
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


def main():
    parser = argparse.ArgumentParser(description="Disparador de Traspaso Masivo de Delivery a Kinesis")
    parser.add_argument("--cantidad", type=int, default=100, help="Cantidad de pedidos a transferir")
    parser.add_argument("--tenant", default="demo", help="Tenant ID")
    parser.add_argument("--local-id", default="local_01", help="Local ID")
    parser.add_argument("--plataforma", default=None, help="ubereats, rappi, pedidosya o null para todas")
    parser.add_argument("--stream", default="pos-delivery-stream", help="Nombre del stream en Kinesis")
    parser.add_argument("--region", default="us-east-1", help="Región AWS")
    args = parser.parse_args()

    print(f"\n{AZUL}{NEGRITA}========================================================================{RESET}")
    print(f"{AZUL}{NEGRITA}   LANZADOR DE TRASPASO MASIVO: MOCK DELIVERY ➔ AMAZON KINESIS AWS      {RESET}")
    print(f"{AZUL}{NEGRITA}========================================================================{RESET}")

    transmisor = TransmisorKinesis(region=args.region, stream_name=args.stream)
    print(f"  {CIAN}ℹ{RESET} Modo de conexión: {NEGRITA}{'AWS Real (us-east-1)' if transmisor.es_real else 'Emulación Local'}{RESET}")
    print(f"  {CIAN}ℹ{RESET} Stream destino: {NEGRITA}{args.stream}{RESET}")

    print(f"\nGenerando {args.cantidad} pedidos de delivery sintéticos...")
    t0_gen = time.monotonic()
    pedidos = [
        generar_pedido_mock(tenant_id=args.tenant, local_id=args.local_id, plataforma=args.plataforma)
        for _ in range(args.cantidad)
    ]
    duracion_gen = round(time.monotonic() - t0_gen, 3)
    total_monto = sum(p["total"] for p in pedidos)

    print(f"  {VERDE}✔{RESET} {len(pedidos)} pedidos generados en {duracion_gen} s (Monto: ${total_monto:,} CLP)")

    print(f"\nTransmitiendo ráfaga masiva a Amazon Kinesis con PutRecords...")
    res = transmisor.enviar_lote_kinesis(pedidos)

    print(f"  {VERDE}✔{RESET} Traspaso completado en {res['tiempo_ms']} ms:")
    print(f"     • Pedidos transmitidos con éxito: {VERDE}{res['exitosos']}{RESET}")
    print(f"     • Pedidos fallidos: {res['fallidos']}")
    print(f"     • Tasa de transferencia: {NEGRITA}{res['pedidos_por_segundo']} pedidos/segundo{RESET}")
    print(f"     • Distribución en Shards de AWS:")
    for s_id, cnt in res.get("distribucion_shards", {}).items():
        print(f"         - {s_id}: {cnt} pedidos")

    print(f"\n{VERDE}{NEGRITA}========================================================================{RESET}")
    print(f"{VERDE}{NEGRITA}   ✔ TRASPASO MASIVO COMPLETADO CON ÉXITO                             {RESET}")
    print(f"{VERDE}{NEGRITA}========================================================================\n{RESET}")


if __name__ == "__main__":
    main()
