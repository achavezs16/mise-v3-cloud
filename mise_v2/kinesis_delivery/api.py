"""API REST con FastAPI para el flujo de delivery con Amazon Kinesis.

Flujo:
    datos mock -> API -> Amazon Kinesis Data Streams -> Base de Datos MISE POS

Incluye endpoints para:
- Crear y consultar el stream de Kinesis.
- Recibir pedidos externos de delivery.
- Disparar simulaciones batch masivas de datos mock.
- Consumir el stream hacia la BD de MISE.
- Consultar los pedidos y estadísticas del sistema.
"""

from __future__ import annotations

from typing import Any, List, Optional
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from kinesis_delivery.consumer import ConsumidorKinesisDelivery
from kinesis_delivery.kinesis_engine import ClienteKinesis, STREAM_NAME_DEFAULT
from kinesis_delivery.mise_db import db_mise
from kinesis_delivery.mock_data import generar_lote_delivery_mock, generar_pedido_delivery_mock

# Instancias compartidas del motor
cliente_kinesis = ClienteKinesis()
consumidor = ConsumidorKinesisDelivery(cliente_kinesis)

app = FastAPI(
    title="MISE POS v3 — API Delivery & Kinesis Data Streams",
    description="""
API para simulación y procesamiento de pedidos de delivery en streaming masivo:
**Datos Mock ➔ API ➔ Amazon Kinesis Data Streams ➔ BD Sistema MISE**
    """,
    version="3.0.0",
)


# --- Esquemas Pydantic ---

class ItemPedidoSchema(BaseModel):
    item_id: Optional[str] = None
    producto_id: str
    nombre: str
    precio_unitario: int
    cantidad: int
    subtotal: int
    notas: Optional[str] = ""


class ClienteSchema(BaseModel):
    nombre: str
    telefono: str
    direccion: str


class RepartidorSchema(BaseModel):
    nombre: str
    vehiculo: str


class PedidoDeliverySchema(BaseModel):
    tenant_id: str = "demo"
    local_id: str = "local_01"
    plataforma: str = Field(description="ubereats, rappi o pedidosya")
    orden_externa_id: str
    cliente: ClienteSchema
    repartidor: RepartidorSchema
    items: List[ItemPedidoSchema]
    subtotal: int
    propina: int = 0
    total: int
    metodo_pago: str = "online_platform"


class CrearStreamSchema(BaseModel):
    nombre_stream: str = STREAM_NAME_DEFAULT
    shards: int = 2


class SimularBatchSchema(BaseModel):
    cantidad: int = Field(default=20, ge=1, le=500, description="Cantidad de pedidos a generar y transmitir")
    tenant_id: str = "demo"
    local_id: str = "local_01"
    plataforma: Optional[str] = Field(default=None, description="ubereats, rappi, pedidosya o null para aleatorio")
    auto_consumir: bool = Field(default=True, description="Si es True, procesa automáticamente hacia la BD de MISE")


# --- Endpoints ---

@app.get("/")
def inicio() -> dict[str, Any]:
    return {
        "sistema": "MISE POS v3 — Streaming de Delivery",
        "flujo": "Datos Mock ➔ API ➔ Amazon Kinesis Data Streams ➔ BD Sistema MISE",
        "documentacion_swagger": "/docs",
        "estado_kinesis": "/api/kinesis/estado",
        "estadisticas_mise": "/api/mise/estadisticas",
    }


@app.get("/api/kinesis/estado")
def estado_kinesis(stream: str = STREAM_NAME_DEFAULT) -> dict[str, Any]:
    """Obtiene el estado, ARN, shards y configuración del stream de Kinesis."""
    try:
        desc = cliente_kinesis.describir_stream(nombre_stream=stream)
        return {
            "stream": stream,
            "estado": desc.get("StreamStatus"),
            "arn": desc.get("StreamARN"),
            "shards": [s["ShardId"] for s in desc.get("Shards", [])],
            "total_shards": len(desc.get("Shards", [])),
            "retencion_horas": desc.get("RetentionPeriodHours"),
            "modo": desc.get("Modo"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando Kinesis: {str(e)}")


@app.post("/api/kinesis/crear-stream")
def crear_stream(body: CrearStreamSchema) -> dict[str, Any]:
    """Crea y provisiona el stream en Amazon Kinesis Data Streams."""
    try:
        resultado = cliente_kinesis.crear_stream(nombre_stream=body.nombre_stream, shards=body.shards)
        return {
            "mensaje": f"Stream '{body.nombre_stream}' provisionado exitosamente.",
            "detalles": resultado,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creando stream en Kinesis: {str(e)}")


@app.post("/api/delivery/pedidos")
def recibir_pedido_delivery(pedido: PedidoDeliverySchema, background_tasks: BackgroundTasks) -> dict[str, Any]:
    """Recibe un pedido de delivery y lo inyecta inmediatamente a Kinesis Data Streams."""
    pedido_dict = pedido.model_dump()
    # Enriquecer con identificadores si faltan
    from kinesis_delivery.mock_data import generar_ulid
    pedido_dict["pedido_id"] = generar_ulid()
    pedido_dict["command_id"] = generar_ulid()
    pedido_dict["event_id"] = generar_ulid()
    pedido_dict["tipo_pedido"] = "delivery"
    from datetime import datetime, timezone
    pedido_dict["recibido_en"] = datetime.now(timezone.utc).isoformat()

    res_kinesis = cliente_kinesis.enviar_batch_pedidos([pedido_dict])
    
    # Procesar en segundo plano hacia la BD de MISE
    background_tasks.add_task(consumidor.procesar_un_ciclo)

    return {
        "mensaje": "Pedido recibido e inyectado a Kinesis Data Streams",
        "pedido_id": pedido_dict["pedido_id"],
        "resultado_kinesis": res_kinesis,
    }


@app.post("/api/delivery/simular-batch")
def simular_batch_pedidos(body: SimularBatchSchema) -> dict[str, Any]:
    """Genera datos mock de delivery y los envía masivamente a Amazon Kinesis Data Streams."""
    # 1. Generar datos mock
    pedidos_mock = generar_lote_delivery_mock(
        cantidad=body.cantidad,
        tenant_id=body.tenant_id,
        local_id=body.local_id,
        plataforma=body.plataforma,
    )

    # 2. Inyectar a Kinesis en lotes PutRecords
    res_kinesis = cliente_kinesis.enviar_batch_pedidos(pedidos_mock)

    # 3. Si auto_consumir está activo, bombear hacia la BD de MISE
    res_consumo = None
    if body.auto_consumir:
        res_consumo = consumidor.procesar_un_ciclo()

    return {
        "etapa_1_mock": {
            "pedidos_generados": len(pedidos_mock),
            "plataformas": list({p["plataforma"] for p in pedidos_mock}),
        },
        "etapa_2_kinesis": res_kinesis,
        "etapa_3_bd_mise": res_consumo,
        "resumen": f"Se procesaron {len(pedidos_mock)} pedidos mock exitosamente a través de Kinesis hacia la BD de MISE.",
    }


@app.post("/api/kinesis/consumir-lote")
def consumir_lote_kinesis(limite: int = Query(default=100, ge=1, le=500)) -> dict[str, Any]:
    """Ejecuta una ronda de lectura desde los shards de Kinesis hacia la BD de MISE."""
    resultado = consumidor.procesar_un_ciclo(limite_por_shard=limite)
    return {
        "mensaje": "Lectura y persistencia de Kinesis completada",
        "resultado": resultado,
    }


@app.get("/api/mise/pedidos")
def listar_pedidos_mise(limite: int = Query(default=20, ge=1, le=100)) -> list[dict[str, Any]]:
    """Consulta los pedidos de delivery almacenados en la tabla 'operacion' de MISE."""
    return db_mise.listar_pedidos_delivery(limite=limite)


@app.get("/api/mise/estadisticas")
def estadisticas_mise() -> dict[str, Any]:
    """Reporte de métricas del sistema: pedidos, items, eventos outbox y tickets de cocina."""
    return {
        "bd_mise": db_mise.obtener_resumen_estadisticas(),
        "consumidor_kinesis": consumidor.stats,
    }


@app.post("/api/mise/limpiar-bd")
def limpiar_base_datos() -> dict[str, Any]:
    """Limpia los datos de la BD de MISE para reiniciar pruebas."""
    db_mise.limpiar()
    return {"mensaje": "Base de datos de MISE reiniciada limpiamente."}
