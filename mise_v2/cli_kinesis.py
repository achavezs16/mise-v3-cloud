#!/usr/bin/env python3
"""CLI para Amazon Kinesis Data Streams en MISE POS v3.

Permite demostrar el uso completo del pipeline:
    datos mock -> API -> Amazon Kinesis Data Streams -> BD Sistema MISE

Uso:
    python cli_kinesis.py crear-stream [--shards 2]
    python cli_kinesis.py simular-batch [--cantidad 50] [--sin-consumo]
    python cli_kinesis.py consumir [--limite 100]
    python cli_kinesis.py ver-bd [--limite 10]
    python cli_kinesis.py iniciar-api [--puerto 8000]
    python cli_kinesis.py demo [--cantidad 30]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Asegurar que el directorio raíz esté en sys.path
RAIZ = Path(__file__).resolve().parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from kinesis_delivery.consumer import ConsumidorKinesisDelivery
from kinesis_delivery.kinesis_engine import ClienteKinesis, STREAM_NAME_DEFAULT
from kinesis_delivery.mise_db import db_mise
from kinesis_delivery.mock_data import generar_lote_delivery_mock


# Formateo de colores para la terminal
class Color:
    VERDE = "\033[92m"
    AZUL = "\033[94m"
    CIAN = "\033[96m"
    AMARILLO = "\033[93m"
    ROJO = "\033[91m"
    NEGRITA = "\033[1m"
    RESET = "\033[0m"


def print_paso(numero: int, titulo: str) -> None:
    print(f"\n{Color.AZUL}{Color.NEGRITA}=== [Paso {numero}] {titulo} ==={Color.RESET}")


def print_ok(msg: str) -> None:
    print(f"  {Color.VERDE}✔{Color.RESET} {msg}")


def print_info(msg: str) -> None:
    print(f"  {Color.CIAN}ℹ{Color.RESET} {msg}")


def print_alerta(msg: str) -> None:
    print(f"  {Color.AMARILLO}!{Color.RESET} {msg}")


def comando_crear_stream(args: argparse.Namespace) -> None:
    """Crea y provisiona el stream en Kinesis."""
    print_paso(1, f"Creando / Verificando Stream en Amazon Kinesis ('{args.stream}')")
    cliente = ClienteKinesis(region_name=args.region, forzar_local=args.local)
    
    print_info(f"Modo detectado: {Color.NEGRITA}{'AWS Real' if cliente.es_real else 'Simulador Local en Memoria'}{Color.RESET}")
    print_info(f"Configuración solicitada: {args.shards} shards en la región {args.region}...")

    t0 = time.monotonic()
    resultado = cliente.crear_stream(nombre_stream=args.stream, shards=args.shards)
    duracion = round(time.monotonic() - t0, 2)

    print_ok(f"Stream activo y listo en {duracion} s")
    print(f"     • Nombre: {resultado.get('StreamName')}")
    print(f"     • Estado: {Color.VERDE}{resultado.get('StreamStatus')}{Color.RESET}")
    print(f"     • ARN: {resultado.get('StreamARN')}")
    print(f"     • Shards activos ({len(resultado.get('Shards', []))}):")
    for s in resultado.get("Shards", []):
        print(f"         - {s['ShardId']}")
    print(f"     • Período de retención: {resultado.get('RetentionPeriodHours')} horas")


def comando_simular_batch(args: argparse.Namespace) -> None:
    """Genera pedidos mock y los transmite a Kinesis."""
    print_paso(1, f"Generando {args.cantidad} pedidos sintéticos de Delivery (Mock)")
    pedidos = generar_lote_delivery_mock(
        cantidad=args.cantidad,
        tenant_id=args.tenant,
        local_id=args.local_id,
        plataforma=args.plataforma,
    )
    plataformas = {p["plataforma"]: 0 for p in pedidos}
    for p in pedidos:
        plataformas[p["plataforma"]] += 1

    print_ok(f"{len(pedidos)} pedidos generados exitosamente.")
    for plat, cnt in plataformas.items():
        print(f"     • {plat}: {cnt} pedidos")

    print_paso(2, f"Transmitiendo a Amazon Kinesis Data Streams ('{args.stream}') con PutRecords")
    cliente = ClienteKinesis(region_name=args.region, forzar_local=args.local)
    res_kinesis = cliente.enviar_batch_pedidos(pedidos, nombre_stream=args.stream)

    print_ok(f"Envío completado en {res_kinesis['tiempo_ms']} ms ({res_kinesis['tasa_pedidos_segundo']} pedidos/s)")
    print(f"     • Registros enviados con éxito: {Color.VERDE}{res_kinesis['enviados_exitosos']}{Color.RESET}")
    print(f"     • Registros fallidos: {res_kinesis['fallidos']}")
    print(f"     • Distribución en shards:")
    for s_id, cnt in res_kinesis["distribucion_shards"].items():
        print(f"         - {s_id}: {cnt} pedidos")

    if not args.sin_consumo:
        print_paso(3, "Consumiendo de Kinesis y escribiendo en la Base de Datos de MISE")
        consumidor = ConsumidorKinesisDelivery(cliente, nombre_stream=args.stream)
        res_consumo = consumidor.procesar_un_ciclo()
        print_ok(f"Procesados {res_consumo['pedidos_leidos_ciclo']} pedidos desde los shards.")
        print(f"     • Nuevos pedidos guardados en BD: {Color.VERDE}{res_consumo['nuevos_insertados_bd']}{Color.RESET}")
        print(f"     • Duplicados detectados e ignorados: {res_consumo['estadisticas_acumuladas']['duplicados']}")


def comando_consumir(args: argparse.Namespace) -> None:
    """Lee registros pendientes de Kinesis y los guarda en la BD de MISE."""
    print_paso(1, f"Leyendo micro-lotes desde Amazon Kinesis ('{args.stream}')")
    cliente = ClienteKinesis(region_name=args.region, forzar_local=args.local)
    consumidor = ConsumidorKinesisDelivery(cliente, nombre_stream=args.stream)
    res = consumidor.procesar_un_ciclo(limite_por_shard=args.limite)

    print_ok("Lectura de shards completada.")
    print(f"     • Pedidos leídos en esta ronda: {res['pedidos_leidos_ciclo']}")
    print(f"     • Nuevos pedidos creados en BD MISE: {res['nuevos_insertados_bd']}")
    print(f"     • Estadísticas totales consumidor:")
    for k, v in res["estadisticas_acumuladas"].items():
        print(f"         - {k}: {v}")


def comando_ver_bd(args: argparse.Namespace) -> None:
    """Muestra los pedidos y estadísticas almacenados en la BD de MISE."""
    print_paso(1, "Consultando Base de Datos Operacional de MISE (Tabla 'operacion')")
    stats = db_mise.obtener_resumen_estadisticas()
    print_info("Resumen de Entidades en BD:")
    print(f"     • Pedidos de Delivery: {Color.VERDE}{stats['pedidos_delivery']}{Color.RESET}")
    print(f"     • Total Ítems de Menú asociados: {stats['items_totales']}")
    print(f"     • Eventos de Auditoría Outbox (EVT#): {stats['eventos_outbox']}")
    print(f"     • Tickets de Cocina Pendientes de Impresión: {stats['tickets_cocina_pendientes']}")
    print(f"     • Monto total facturado: ${stats['monto_total_recaudado_clp']:,} CLP")

    pedidos = db_mise.listar_pedidos_delivery(limite=args.limite)
    if not pedidos:
        print_alerta("No hay pedidos de delivery en la base de datos todavía.")
        return

    print_info(f"\nÚltimos {len(pedidos)} pedidos registrados:")
    print("-" * 95)
    print(f"{'PEDIDO ID':<28} | {'PLATAFORMA':<12} | {'ORDEN EXT':<15} | {'ITEMS':<6} | {'TOTAL CLP':<10} | {'CLIENTE'}")
    print("-" * 95)
    for p in pedidos:
        cliente_nom = p.get("cliente", {}).get("nombre", "N/A")
        total_fmt = f"${p.get('total', 0):,}"
        print(f"{p.get('PK').split('#')[-1]:<28} | {p.get('plataforma', 'N/A'):<12} | {p.get('orden_externa_id', 'N/A'):<15} | {p.get('total_items', 1):<6} | {total_fmt:<10} | {cliente_nom}")
    print("-" * 95)


def comando_verificar_aws(args: argparse.Namespace) -> None:
    """Verifica credenciales, conecta a AWS Kinesis y DynamoDB y va a buscar los datos."""
    from scripts.conectar_y_verificar_aws import main as verificar_main
    verificar_main(args)


def comando_iniciar_api(args: argparse.Namespace) -> None:
    """Inicia el servidor API FastAPI."""
    import uvicorn
    print_paso(1, f"Iniciando API REST de Delivery & Kinesis en http://{args.host}:{args.puerto}")
    print_ok(f"Documentación Swagger interactiva disponible en: {Color.VERDE}http://{args.host}:{args.puerto}/docs{Color.RESET}")
    print_info("Presiona Ctrl+C para detener el servidor.\n")
    uvicorn.run("kinesis_delivery.api:app", host=args.host, port=args.puerto, reload=False)


def comando_demo(args: argparse.Namespace) -> None:
    """Ejecuta una demostración completa paso a paso de punta a punta."""
    print(f"\n{Color.VERDE}{Color.NEGRITA}========================================================================{Color.RESET}")
    print(f"{Color.VERDE}{Color.NEGRITA}      DEMOSTRACIÓN INTEGRAL: PIPELINE AMAZON KINESIS - MISE POS v3      {Color.RESET}")
    print(f"{Color.VERDE}{Color.NEGRITA}    Flujo: Datos Mock ➔ API ➔ Kinesis Data Streams ➔ BD Sistema MISE    {Color.RESET}")
    print(f"{Color.VERDE}{Color.NEGRITA}========================================================================{Color.RESET}")

    # 1. Crear Stream
    comando_crear_stream(args)

    # 2. Generar y Enviar Pedidos Mock Batch
    comando_simular_batch(args)

    # 3. Ver BD
    comando_ver_bd(args)

    print(f"\n{Color.VERDE}{Color.NEGRITA}========================================================================{Color.RESET}")
    print(f"{Color.VERDE}{Color.NEGRITA}   ✔ DEMOSTRACIÓN COMPLETADA CON ÉXITO: KINESIS DATA STREAMS OPERATIVO   {Color.RESET}")
    print(f"{Color.VERDE}{Color.NEGRITA}========================================================================\n{Color.RESET}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CLI para Amazon Kinesis Data Streams en MISE POS v3",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--stream", default=STREAM_NAME_DEFAULT, help="Nombre del stream de Kinesis")
    parser.add_argument("--region", default="us-east-1", help="Región AWS")
    parser.add_argument("--local", action="store_true", help="Forzar uso del simulador local en memoria")

    subparsers = parser.add_subparsers(dest="subcomando", required=True)

    # Subcomando: crear-stream
    p_crear = subparsers.add_parser("crear-stream", help="Crea y provisiona el stream en Kinesis")
    p_crear.add_argument("--shards", type=int, default=2, help="Cantidad de shards a aprovisionar")
    p_crear.set_defaults(func=comando_crear_stream)

    # Subcomando: simular-batch
    p_sim = subparsers.add_parser("simular-batch", help="Genera datos mock de delivery y los envía a Kinesis")
    p_sim.add_argument("--cantidad", type=int, default=30, help="Cantidad de pedidos sintéticos")
    p_sim.add_argument("--tenant", default="demo", help="Tenant ID")
    p_sim.add_argument("--local-id", default="local_01", help="Local ID")
    p_sim.add_argument("--plataforma", default=None, help="Filtrar por plataforma (ubereats, rappi, pedidosya)")
    p_sim.add_argument("--sin-consumo", action="store_true", help="No consumir hacia la BD inmediatamente")
    p_sim.set_defaults(func=comando_simular_batch)

    # Subcomando: consumir
    p_cons = subparsers.add_parser("consumir", help="Lee de Kinesis y escribe en la BD de MISE")
    p_cons.add_argument("--limite", type=int, default=100, help="Límite de lectura por shard")
    p_cons.set_defaults(func=comando_consumir)

    # Subcomando: ver-bd
    p_ver = subparsers.add_parser("ver-bd", help="Consulta los pedidos registrados en la BD de MISE")
    p_ver.add_argument("--limite", type=int, default=15, help="Cantidad de pedidos a listar")
    p_ver.set_defaults(func=comando_ver_bd)

    # Subcomando: iniciar-api
    p_api = subparsers.add_parser("iniciar-api", help="Inicia la API REST de FastAPI")
    p_api.add_argument("--host", default="127.0.0.1", help="Host a escuchar")
    p_api.add_argument("--puerto", type=int, default=8000, help="Puerto a escuchar")
    p_api.set_defaults(func=comando_iniciar_api)

    # Subcomando: demo
    p_demo = subparsers.add_parser("demo", help="Ejecuta la demostración completa del pipeline de punta a punta")
    p_demo.add_argument("--shards", type=int, default=2, help="Shards para Kinesis")
    p_demo.add_argument("--cantidad", type=int, default=30, help="Cantidad de pedidos para la demo")
    p_demo.add_argument("--tenant", default="demo", help="Tenant ID")
    p_demo.add_argument("--local-id", default="local_01", help="Local ID")
    p_demo.add_argument("--plataforma", default=None, help="Filtrar plataforma")
    p_demo.add_argument("--sin-consumo", action="store_false", dest="sin_consumo", default=False)
    p_demo.add_argument("--limite", type=int, default=15)
    p_demo.set_defaults(func=comando_demo)

    # Subcomando: verificar-aws
    p_aws = subparsers.add_parser("verificar-aws", help="Conecta con AWS real, transmite y va a buscar los datos a Kinesis y DynamoDB")
    p_aws.add_argument("--tabla", default="operacion", help="Tabla DynamoDB de MISE")
    p_aws.add_argument("--local-fallback", action="store_true", help="Usar simulador si no hay credenciales")
    p_aws.set_defaults(func=comando_verificar_aws)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
