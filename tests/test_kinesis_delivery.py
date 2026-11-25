"""Pruebas unitarias y de integración para el pipeline de Amazon Kinesis y Delivery.

Verifica:
1. Generación y coherencia aritmética de pedidos mock.
2. Motor Kinesis (creación de stream, particionamiento MD5 por shard y PutRecords).
3. Transacciones atómicas, Outbox e idempotencia en la base de datos de MISE.
4. Consumo de micro-lotes y bombeo Kinesis -> BD MISE.
5. Endpoints HTTP de la API FastAPI.
"""

import pytest
from fastapi.testclient import TestClient

from kinesis_delivery.api import app
from kinesis_delivery.consumer import ConsumidorKinesisDelivery
from kinesis_delivery.kinesis_engine import ClienteKinesis
from kinesis_delivery.mise_db import AlmacenMiseDB
from kinesis_delivery.mock_data import generar_lote_delivery_mock, generar_pedido_delivery_mock


# --- 1. Pruebas del Generador Mock ---

def test_generador_pedido_mock_formato_y_matematica():
    pedido = generar_pedido_delivery_mock(tenant_id="test_t", local_id="test_l", plataforma="ubereats")
    
    assert pedido["tenant_id"] == "test_t"
    assert pedido["local_id"] == "test_l"
    assert pedido["plataforma"] == "ubereats"
    assert pedido["tipo_pedido"] == "delivery"
    assert len(pedido["pedido_id"]) == 26  # ULID
    assert len(pedido["command_id"]) == 26
    assert len(pedido["event_id"]) == 26
    assert len(pedido["items"]) >= 1

    # Comprobar suma aritmética exacta
    subtotal_calculado = sum(it["precio_unitario"] * it["cantidad"] for it in pedido["items"])
    assert pedido["subtotal"] == subtotal_calculado
    assert pedido["total"] == pedido["subtotal"] + pedido["propina"]


def test_generador_lote_mock():
    lote = generar_lote_delivery_mock(cantidad=15, tenant_id="demo", local_id="local_01")
    assert len(lote) == 15
    ids_pedidos = {p["pedido_id"] for p in lote}
    assert len(ids_pedidos) == 15  # Todos los ULIDs deben ser únicos


# --- 2. Pruebas del Motor Kinesis ---

def test_motor_kinesis_creacion_y_envio_batch():
    cliente = ClienteKinesis(forzar_local=True)
    nombre_stream = "test-stream-delivery"
    desc = cliente.crear_stream(nombre_stream=nombre_stream, shards=2)

    assert desc["StreamName"] == nombre_stream
    assert desc["StreamStatus"] == "ACTIVE"
    assert len(desc["Shards"]) == 2

    # Enviar lote de 20 pedidos
    pedidos = generar_lote_delivery_mock(cantidad=20, tenant_id="t1", local_id="l1")
    res_envio = cliente.enviar_batch_pedidos(pedidos, nombre_stream=nombre_stream)

    assert res_envio["total_pedidos"] == 20
    assert res_envio["enviados_exitosos"] == 20
    assert res_envio["fallidos"] == 0
    assert sum(res_envio["distribucion_shards"].values()) == 20

    # Leer registros desde el shard
    shards = cliente.obtener_todos_los_shards(nombre_stream)
    total_leidos = 0
    for s in shards:
        registros, _ = cliente.leer_lote(shard_id=s, nombre_stream=nombre_stream, tipo_iterador="TRIM_HORIZON")
        total_leidos += len(registros)

    assert total_leidos == 20


# --- 3. Pruebas de Transacciones e Idempotencia en BD MISE ---

def test_almacen_mise_db_idempotencia_y_outbox(tmp_path):
    archivo_db = tmp_path / "test_db.json"
    db = AlmacenMiseDB(archivo_persistencia=archivo_db)
    db.limpiar()

    pedido = generar_pedido_delivery_mock(tenant_id="t_val", local_id="l_val")
    res1 = db.aplicar_transaccion_delivery(pedido)
    assert res1["resultado"] == "EXITO"

    # 1. Comprobar que reintentar el mismo command_id es ignorado (Idempotencia CMD)
    res2 = db.aplicar_transaccion_delivery(pedido)
    assert res2["resultado"] == "DUPLICADO_IGNORADO"

    # 2. Comprobar que una orden externa repetida se detecta
    pedido_duplicado = generar_pedido_delivery_mock(tenant_id="t_val", local_id="l_val")
    pedido_duplicado["orden_externa_id"] = pedido["orden_externa_id"]
    pedido_duplicado["plataforma"] = pedido["plataforma"]
    res3 = db.aplicar_transaccion_delivery(pedido_duplicado)
    assert res3["resultado"] == "ORDEN_YA_EXISTE"

    # 3. Comprobar que se crearon los ítems esperados
    stats = db.obtener_resumen_estadisticas()
    assert stats["pedidos_delivery"] == 1
    assert stats["eventos_outbox"] == 1
    assert stats["tickets_cocina_pendientes"] == 1


# --- 4. Pruebas del Consumidor ---

def test_consumidor_kinesis_hacia_bd(tmp_path):
    cliente = ClienteKinesis(forzar_local=True)
    nombre_stream = "stream-consumidor-test"
    cliente.crear_stream(nombre_stream, shards=2)

    pedidos = generar_lote_delivery_mock(cantidad=8)
    cliente.enviar_batch_pedidos(pedidos, nombre_stream=nombre_stream)

    consumidor = ConsumidorKinesisDelivery(cliente, nombre_stream=nombre_stream)
    res = consumidor.procesar_un_ciclo()

    assert res["pedidos_leidos_ciclo"] == 8
    assert res["estadisticas_acumuladas"]["total_leidos"] == 8
    assert res["estadisticas_acumuladas"]["errores"] == 0


# --- 5. Pruebas de la API FastAPI ---

def test_api_fastapi_endpoints():
    client = TestClient(app)

    # Estado Kinesis
    r_estado = client.get("/api/kinesis/estado")
    assert r_estado.status_code == 200
    assert r_estado.json()["estado"] == "ACTIVE"

    # Simular Batch
    r_batch = client.post("/api/delivery/simular-batch", json={"cantidad": 5, "auto_consumir": True})
    assert r_batch.status_code == 200
    datos = r_batch.json()
    assert datos["etapa_1_mock"]["pedidos_generados"] == 5
    assert datos["etapa_2_kinesis"]["enviados_exitosos"] == 5

    # Listar pedidos en BD MISE
    r_pedidos = client.get("/api/mise/pedidos")
    assert r_pedidos.status_code == 200
    assert len(r_pedidos.json()) >= 1

    # Estadísticas
    r_stats = client.get("/api/mise/estadisticas")
    assert r_stats.status_code == 200
    assert r_stats.json()["bd_mise"]["pedidos_delivery"] >= 1
