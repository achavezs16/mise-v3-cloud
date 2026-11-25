"""Consumidor de Amazon Kinesis Data Streams -> Base de Datos MISE POS.

Equivalente a la función Lambda `consumidor-delivery-kinesis`:
1. Lee micro-lotes de registros desde los shards del stream.
2. Deserializa el payload de cada pedido de delivery.
3. Aplica la transacción atómica en la tabla `operacion` de DynamoDB (MiseDB).
4. Registra métricas de procesamiento y garantiza idempotencia.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from kinesis_delivery.kinesis_engine import ClienteKinesis, STREAM_NAME_DEFAULT
from kinesis_delivery.mise_db import db_mise

log = logging.getLogger("kinesis.consumer")


class ConsumidorKinesisDelivery:
    """Worker que extrae pedidos de Kinesis y los persiste en la BD de MISE."""

    def __init__(self, cliente_kinesis: ClienteKinesis, nombre_stream: str = STREAM_NAME_DEFAULT) -> None:
        self.cliente = cliente_kinesis
        self.nombre_stream = nombre_stream
        self._iteradores_shard: dict[str, str | None] = {}
        self._detener_daemon = threading.Event()
        self._hilo_daemon: threading.Thread | None = None
        self.stats = {
            "total_leidos": 0,
            "procesados_exito": 0,
            "duplicados": 0,
            "errores": 0,
        }

    def procesar_un_ciclo(self, limite_por_shard: int = 100) -> dict[str, Any]:
        """Ejecuta una pasada de lectura sobre todos los shards disponibles."""
        shards = self.cliente.obtener_todos_los_shards(self.nombre_stream)
        pedidos_ciclo = 0
        nuevos_insertados = 0

        for s_id in shards:
            iterador = self._iteradores_shard.get(s_id)
            registros, siguiente_iterador = self.cliente.leer_lote(
                shard_id=s_id,
                shard_iterator=iterador,
                nombre_stream=self.nombre_stream,
                tipo_iterador="TRIM_HORIZON" if not iterador else "LATEST",
                limite=limite_por_shard,
            )
            self._iteradores_shard[s_id] = siguiente_iterador

            for item_kinesis in registros:
                pedido = item_kinesis["pedido"]
                self.stats["total_leidos"] += 1
                pedidos_ciclo += 1

                try:
                    res = db_mise.aplicar_transaccion_delivery(pedido)
                    if res["resultado"] == "EXITO":
                        self.stats["procesados_exito"] += 1
                        nuevos_insertados += 1
                    else:
                        self.stats["duplicados"] += 1
                except Exception as e:
                    log.error(f"Error procesando pedido en BD MISE: {e}")
                    self.stats["errores"] += 1

        return {
            "shards_leidos": len(shards),
            "pedidos_leidos_ciclo": pedidos_ciclo,
            "nuevos_insertados_bd": nuevos_insertados,
            "estadisticas_acumuladas": dict(self.stats),
        }

    def iniciar_worker_daemon(self, intervalo_segundos: float = 1.0) -> None:
        """Inicia un hilo en segundo plano que consume continuamente de Kinesis."""
        if self._hilo_daemon and self._hilo_daemon.is_alive():
            return

        self._detener_daemon.clear()

        def _bucle():
            log.info("Consumidor Kinesis daemon iniciado...")
            while not self._detener_daemon.is_set():
                try:
                    self.procesar_un_ciclo()
                except Exception as e:
                    log.error(f"Error en bucle consumidor Kinesis: {e}")
                time.sleep(intervalo_segundos)

        self._hilo_daemon = threading.Thread(target=_bucle, daemon=True)
        self._hilo_daemon.start()

    def detener_worker_daemon(self) -> None:
        self._detener_daemon.set()
        if self._hilo_daemon:
            self._hilo_daemon.join(timeout=2.0)
            log.info("Consumidor Kinesis daemon detenido.")
