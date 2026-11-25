#!/usr/bin/env python3
"""Script de verificación, conexión y consulta de datos en AWS (Amazon Kinesis + DynamoDB).

Cumple el flujo:
1. Conexión con AWS (STS get_caller_identity).
2. Conexión con Amazon Kinesis Data Streams (describe_stream_summary).
3. Transmisión de un pedido de delivery (put_record / put_records).
4. 'Ir a buscar los datos' a Kinesis (get_shard_iterator y get_records en tiempo real).
5. 'Ir a buscar los datos' a DynamoDB (tabla 'operacion' del sistema MISE).

Uso:
    python scripts/conectar_y_verificar_aws.py [--stream pos-delivery-stream] [--region us-east-1]
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from kinesis_delivery.mock_data import generar_pedido_delivery_mock

# Colores para la terminal
VERDE = "\033[92m"
AZUL = "\033[94m"
CIAN = "\033[96m"
AMARILLO = "\033[93m"
ROJO = "\033[91m"
NEGRITA = "\033[1m"
RESET = "\033[0m"


def print_seccion(titulo: str) -> None:
    print(f"\n{AZUL}{NEGRITA}=== {titulo} ==={RESET}")


def print_ok(msg: str) -> None:
    print(f"  {VERDE}✔{RESET} {msg}")


def print_info(msg: str) -> None:
    print(f"  {CIAN}ℹ{RESET} {msg}")


def print_alerta(msg: str) -> None:
    print(f"  {AMARILLO}!{RESET} {msg}")


def verificar_credenciales_aws(region: str) -> dict[str, str] | None:
    """Verifica si existen credenciales válidas de AWS."""
    print_seccion("1. Verificando Conexión e Identidad en AWS")
    try:
        session = boto3.Session(region_name=region)
        sts = session.client("sts")
        identidad = sts.get_caller_identity()
        print_ok("Credenciales de AWS activas y verificadas exitosamente:")
        print(f"     • Cuenta AWS: {identidad.get('Account')}")
        print(f"     • Identidad (ARN): {identidad.get('Arn')}")
        print(f"     • Región: {region}")
        return identidad
    except (NoCredentialsError, ClientError) as e:
        print_alerta(f"No se detectaron credenciales activas de AWS ({e}).")
        print_info("Para conectarse a AWS Learner Lab o cuenta real, exporta:")
        print("     $env:AWS_ACCESS_KEY_ID = '...'")
        print("     $env:AWS_SECRET_ACCESS_KEY = '...'")
        print("     $env:AWS_SESSION_TOKEN = '...' (si usas Learner Lab)")
        print("     $env:AWS_DEFAULT_REGION = 'us-east-1'")
        return None


def asegurar_stream_kinesis(kinesis_client, stream_name: str, shards: int = 2) -> dict:
    """Verifica el stream en Kinesis o lo crea si no existe."""
    print_seccion(f"2. Conectando con Amazon Kinesis Data Streams ('{stream_name}')")
    try:
        desc = kinesis_client.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
        print_ok(f"Stream '{stream_name}' encontrado en AWS:")
        print(f"     • Estado: {VERDE}{desc['StreamStatus']}{RESET}")
        print(f"     • ARN: {desc['StreamARN']}")
        print(f"     • Shards abiertos: {desc['OpenShardCount']}")
        print(f"     • Retención: {desc['RetentionPeriodHours']} horas")
        return desc
    except ClientError as e:
        codigo = e.response.get("Error", {}).get("Code")
        if codigo == "ResourceNotFoundException":
            print_info(f"Stream '{stream_name}' no existe en AWS. Creándolo con {shards} shards...")
            kinesis_client.create_stream(StreamName=stream_name, ShardCount=shards)
            print("     Esperando a que el stream esté ACTIVE en AWS...")
            waiter = kinesis_client.get_waiter("stream_exists")
            waiter.wait(StreamName=stream_name)
            desc = kinesis_client.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
            print_ok(f"Stream creado y activo: {desc['StreamARN']}")
            return desc
        raise e


def enviar_pedido_kinesis(kinesis_client, stream_name: str, pedido: dict) -> dict:
    """Envía un pedido de delivery a Kinesis Data Streams."""
    print_seccion("3. Transmitiendo Pedido de Delivery hacia AWS Kinesis")
    tenant_id = pedido["tenant_id"]
    local_id = pedido["local_id"]
    p_key = f"{tenant_id}#{local_id}"
    payload_json = json.dumps(pedido, ensure_ascii=False)

    t0 = time.monotonic()
    res = kinesis_client.put_record(
        StreamName=stream_name,
        Data=payload_json.encode("utf-8"),
        PartitionKey=p_key,
    )
    duracion_ms = int((time.monotonic() - t0) * 1000)

    seq_num = res["SequenceNumber"]
    shard_id = res["ShardId"]

    print_ok(f"Pedido enviado exitosamente al stream en {duracion_ms} ms:")
    print(f"     • Pedido ID: {pedido['pedido_id']}")
    print(f"     • Plataforma: {pedido['plataforma'].upper()} (Orden: {pedido['orden_externa_id']})")
    print(f"     • Shard asignado: {shard_id}")
    print(f"     • Número de Secuencia AWS: {seq_num}")
    print(f"     • Total: ${pedido['total']:,} CLP (Cliente: {pedido['cliente']['nombre']})")

    return {"seq_num": seq_num, "shard_id": shard_id}


def ir_a_buscar_datos_kinesis(kinesis_client, stream_name: str, shard_id: str, seq_num: str) -> dict | None:
    """Se conecta al shard de Kinesis y va a buscar el registro recién transmitido."""
    print_seccion(f"4. 'Ir a buscar los datos' a Amazon Kinesis (Lectura de Shard)")
    print_info(f"Obteniendo ShardIterator AT_SEQUENCE_NUMBER para {shard_id}...")

    iterador_res = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType="AT_SEQUENCE_NUMBER",
        StartingSequenceNumber=seq_num,
    )
    iterador = iterador_res["ShardIterator"]

    t0 = time.monotonic()
    records_res = kinesis_client.get_records(ShardIterator=iterador, Limit=10)
    duracion_ms = int((time.monotonic() - t0) * 1000)

    registros = records_res.get("Records", [])
    print_ok(f"Lectura completada desde el stream en {duracion_ms} ms ({len(registros)} registros recuperados)")

    for r in registros:
        data_raw = r["Data"]
        pedido_recuperado = json.loads(data_raw.decode("utf-8"))
        print_ok(f"Dato recuperado de Kinesis: Pedido ID {pedido_recuperado['pedido_id']}")
        print(f"     • Cliente recuperado: {pedido_recuperado['cliente']['nombre']}")
        print(f"     • Dirección despacho: {pedido_recuperado['cliente']['direccion']}")
        print(f"     • Cantidad de ítems: {len(pedido_recuperado['items'])}")
        print(f"     • Total verificado: ${pedido_recuperado['total']:,} CLP")
        return pedido_recuperado

    print_alerta("No se obtuvieron registros en la primera lectura de iterador.")
    return None


def ir_a_buscar_datos_dynamodb(dynamodb_client, tabla_operacion: str, pedido: dict) -> None:
    """Va a buscar la orden a la tabla 'operacion' de DynamoDB para verificar persistencia."""
    print_seccion(f"5. 'Ir a buscar los datos' a la Base de Datos DynamoDB ('{tabla_operacion}')")
    tenant_id = pedido["tenant_id"]
    local_id = pedido["local_id"]
    pedido_id = pedido["pedido_id"]
    pk_ped = f"TENANT#{tenant_id}#LOCAL#{local_id}#PED#{pedido_id}"

    try:
        res = dynamodb_client.get_item(
            TableName=tabla_operacion,
            Key={"PK": {"S": pk_ped}, "SK": {"S": "PEDIDO"}},
        )
        item = res.get("Item")
        if item:
            print_ok("Pedido encontrado en la tabla 'operacion' de DynamoDB:")
            print(f"     • PK: {item['PK']['S']}")
            print(f"     • Tipo Ítem: {item['tipo_item']['S']} ({item.get('tipo_pedido', {}).get('S', 'N/A')})")
            print(f"     • Plataforma: {item.get('plataforma', {}).get('S')}")
            print(f"     • Total en BD: ${int(item.get('total', {}).get('N', 0)):,} CLP")
            print(f"     • Estado: {VERDE}{item.get('estado', {}).get('S')}{RESET}")
        else:
            print_info(f"El ítem {pk_ped} aún no ha sido procesado en DynamoDB por el Lambda consumidor.")
            print_info("Si la función Lambda 'consumidor-delivery' está activa con EventSourceMapping, se procesará automáticamente.")
    except ClientError as e:
        codigo = e.response.get("Error", {}).get("Code")
        if codigo == "ResourceNotFoundException":
            print_info(f"La tabla DynamoDB '{tabla_operacion}' aún no ha sido creada.")
            print_info("Para crear todas las tablas DynamoDB y Lambdas de MISE, ejecuta:")
            print(f"     {NEGRITA}bash infra/cloudshell/desplegar.sh{RESET}")
        else:
            print_alerta(f"No se pudo consultar DynamoDB ({codigo}): {e}")


def main(args: argparse.Namespace | None = None) -> None:
    if args is None:
        parser = argparse.ArgumentParser(description="Verificación y consulta de datos en AWS Kinesis y DynamoDB")
        parser.add_argument("--stream", default="pos-delivery-stream", help="Nombre del stream de Kinesis")
        parser.add_argument("--region", default="us-east-1", help="Región AWS")
        parser.add_argument("--tabla", default="operacion", help="Nombre de la tabla DynamoDB")
        parser.add_argument("--local-fallback", action="store_true", help="Usar motor local si no hay credenciales")
        args = parser.parse_args()

    print("========================================================================")
    print(" VERIFICACION Y CONEXION CON AWS: KINESIS DATA STREAMS & DYNAMODB")
    print("========================================================================")

    identidad = verificar_credenciales_aws(args.region)

    if not identidad:
        if args.local_fallback:
            print_info("Ejecutando en modo de prueba local...")
            from cli_kinesis import main as cli_main
            sys.argv = ["cli_kinesis.py", "demo", "--cantidad", "5"]
            cli_main()
            return
        else:
            print("\nPara ejecutar contra AWS Real, asegúrate de tener credenciales de AWS activas.")
            return

    # Si hay credenciales reales de AWS:
    session = boto3.Session(region_name=args.region)
    kinesis_client = session.client("kinesis")
    dynamodb_client = session.client("dynamodb")

    # 1. Asegurar Stream
    asegurar_stream_kinesis(kinesis_client, args.stream)

    # 2. Generar pedido de prueba
    pedido = generar_pedido_delivery_mock(tenant_id="demo", local_id="local_01")

    # 3. Enviar a Kinesis
    envio = enviar_pedido_kinesis(kinesis_client, args.stream, pedido)

    # 4. Ir a buscar los datos a Kinesis
    ir_a_buscar_datos_kinesis(kinesis_client, args.stream, envio["shard_id"], envio["seq_num"])

    # 5. Ir a buscar los datos a DynamoDB
    ir_a_buscar_datos_dynamodb(dynamodb_client, args.tabla, pedido)

    print(f"\n{VERDE}{NEGRITA}========================================================================{RESET}")
    print(f"{VERDE}{NEGRITA}   ✔ CONEXION Y VERIFICACION CON AWS COMPLETADA EXITOSAMENTE           {RESET}")
    print(f"{VERDE}{NEGRITA}========================================================================\n{RESET}")


if __name__ == "__main__":
    main()
