"""Adaptador y motor de Amazon Kinesis Data Streams.

Soporta dos modos de operación transparentes:
1. Modo AWS Real (usando boto3 y credenciales de AWS activas).
2. Modo Simulador Local (emulación en memoria con la misma semántica de shards,
   particionamiento MD5, iteradores y límite de 500 registros por lote).
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import time
from typing import Any

log = logging.getLogger("kinesis.engine")

# Límite oficial de la API PutRecords de AWS Kinesis
MAX_RECORDS_PER_PUT = 500
STREAM_NAME_DEFAULT = "pos-delivery-stream"


class MotorKinesisSimulado:
    """Emulador en memoria de Amazon Kinesis Data Streams."""

    def __init__(self, nombre_stream: str = STREAM_NAME_DEFAULT, shards_count: int = 2) -> None:
        self.nombre_stream = nombre_stream
        self.shards_count = shards_count
        self.arn = f"arn:aws:kinesis:us-east-1:123456789012:stream/{nombre_stream}"
        self.estado = "ACTIVE"
        self.creado_en = time.time()
        self.shards: dict[str, list[dict[str, Any]]] = {
            f"shardId-{i:012d}": [] for i in range(shards_count)
        }
        self._secuencia_actual = 1
        log.info(f"[Kinesis Simulado] Stream '{nombre_stream}' inicializado con {shards_count} shards.")

    def _calcular_shard(self, partition_key: str) -> str:
        """Determina el shard correspondiente mediante hashing MD5 de la clave de partición."""
        hash_val = int(hashlib.md5(partition_key.encode("utf-8")).hexdigest(), 16)
        indice = hash_val % self.shards_count
        return f"shardId-{indice:012d}"

    def describir(self) -> dict[str, Any]:
        return {
            "StreamName": self.nombre_stream,
            "StreamARN": self.arn,
            "StreamStatus": self.estado,
            "StreamModeDetails": {"StreamMode": "PROVISIONED"},
            "Shards": [
                {
                    "ShardId": s_id,
                    "SequenceNumberRange": {"StartingSequenceNumber": "0"},
                }
                for s_id in self.shards.keys()
            ],
            "RetentionPeriodHours": 24,
            "Modo": "SIMULADO_LOCAL",
        }

    def put_records(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        records_result = []
        fallos = 0

        for r in records:
            p_key = r["PartitionKey"]
            data_bytes = r["Data"]
            shard_id = self._calcular_shard(p_key)
            seq_num = f"{self._secuencia_actual:030d}"
            self._secuencia_actual += 1

            self.shards[shard_id].append({
                "SequenceNumber": seq_num,
                "PartitionKey": p_key,
                "Data": data_bytes,
                "ApproximateArrivalTimestamp": time.time(),
            })

            records_result.append({
                "ShardId": shard_id,
                "SequenceNumber": seq_num,
            })

        return {
            "FailedRecordCount": fallos,
            "Records": records_result,
        }

    def get_shard_iterator(self, shard_id: str, iterator_type: str = "LATEST") -> str:
        return f"{shard_id}:{len(self.shards.get(shard_id, [])) if iterator_type == 'LATEST' else 0}"

    def get_records(self, shard_iterator: str, limit: int = 100) -> dict[str, Any]:
        partes = shard_iterator.split(":")
        shard_id = partes[0]
        pos = int(partes[1]) if len(partes) > 1 else 0

        registros_shard = self.shards.get(shard_id, [])
        lote = registros_shard[pos : pos + limit]
        siguiente_pos = pos + len(lote)
        siguiente_iterador = f"{shard_id}:{siguiente_pos}"

        return {
            "Records": lote,
            "NextShardIterator": siguiente_iterador,
            "MillisBehindLatest": 0,
        }


class ClienteKinesis:
    """Cliente unificado con autodetección de AWS real o emulador local."""

    def __init__(self, region_name: str = "us-east-1", forzar_local: bool = False) -> None:
        self.region_name = region_name
        self.forzar_local = forzar_local
        self.es_real = False
        self._cliente_boto = None
        self._simulador: MotorKinesisSimulado | None = None

        if not forzar_local:
            try:
                import boto3
                # Comprobar si hay credenciales válidas
                session = boto3.Session(region_name=self.region_name)
                sts = session.client("sts")
                sts.get_caller_identity()
                self._cliente_boto = session.client("kinesis")
                self.es_real = True
                log.info(f"[Kinesis] Conectado exitosamente a AWS Kinesis en {self.region_name}")
            except Exception as e:
                log.warning(f"[Kinesis] No hay credenciales de AWS activas ({e}). Utilizando simulador Kinesis en memoria.")
                self.es_real = False

        if not self.es_real:
            self._simulador = MotorKinesisSimulado()

    def crear_stream(self, nombre_stream: str = STREAM_NAME_DEFAULT, shards: int = 2) -> dict[str, Any]:
        """Crea el stream si no existe y espera a que esté activo."""
        if self.es_real:
            try:
                self._cliente_boto.create_stream(
                    StreamName=nombre_stream,
                    ShardCount=shards,
                    StreamModeDetails={"StreamMode": "PROVISIONED"},
                )
                log.info(f"Creando stream '{nombre_stream}' con {shards} shards en AWS...")
            except self._cliente_boto.exceptions.ResourceInUseException:
                log.info(f"El stream '{nombre_stream}' ya existe en AWS.")

            # Esperar a que el stream esté ACTIVE
            while True:
                desc = self._cliente_boto.describe_stream_summary(StreamName=nombre_stream)
                estado = desc["StreamDescriptionSummary"]["StreamStatus"]
                if estado == "ACTIVE":
                    log.info(f"El stream '{nombre_stream}' está ACTIVE en AWS.")
                    break
                time.sleep(1)
            return self.describir_stream(nombre_stream)
        else:
            if not self._simulador or self._simulador.nombre_stream != nombre_stream:
                self._simulador = MotorKinesisSimulado(nombre_stream=nombre_stream, shards_count=shards)
            return self._simulador.describir()

    def describir_stream(self, nombre_stream: str = STREAM_NAME_DEFAULT) -> dict[str, Any]:
        """Obtiene la descripción técnica y el estado del stream."""
        if self.es_real:
            res = self._cliente_boto.describe_stream(StreamName=nombre_stream)
            desc = res["StreamDescription"]
            return {
                "StreamName": desc["StreamName"],
                "StreamARN": desc["StreamARN"],
                "StreamStatus": desc["StreamStatus"],
                "StreamModeDetails": desc.get("StreamModeDetails", {"StreamMode": "PROVISIONED"}),
                "Shards": desc["Shards"],
                "RetentionPeriodHours": desc["RetentionPeriodHours"],
                "Modo": "AWS_REAL",
            }
        else:
            return self._simulador.describir()

    def enviar_batch_pedidos(
        self,
        pedidos: list[dict[str, Any]],
        nombre_stream: str = STREAM_NAME_DEFAULT,
    ) -> dict[str, Any]:
        """Envía una lista de pedidos a Kinesis en lotes de hasta 500 registros con PutRecords."""
        total_enviados = 0
        total_fallidos = 0
        detalles_shards: dict[str, int] = {}
        tiempo_inicio = time.monotonic()

        # Particionar en sub-lotes de máximo 500 registros
        for i in range(0, len(pedidos), MAX_RECORDS_PER_PUT):
            sub_lote = pedidos[i : i + MAX_RECORDS_PER_PUT]
            kinesis_records = []

            for p in sub_lote:
                # Partición clave: tenant_id#local_id para mantener orden por local
                p_key = f"{p.get('tenant_id', 'demo')}#{p.get('local_id', 'local_01')}"
                payload_json = json.dumps(p, ensure_ascii=False)
                data_bytes = payload_json.encode("utf-8")

                kinesis_records.append({
                    "Data": data_bytes,
                    "PartitionKey": p_key,
                })

            if self.es_real:
                res = self._cliente_boto.put_records(
                    StreamName=nombre_stream,
                    Records=kinesis_records,
                )
            else:
                res = self._simulador.put_records(records=kinesis_records)

            fallidos_sub = res.get("FailedRecordCount", 0)
            total_fallidos += fallidos_sub
            exitosos_sub = len(sub_lote) - fallidos_sub
            total_enviados += exitosos_sub

            for r in res.get("Records", []):
                shard_id = r.get("ShardId", "desconocido")
                detalles_shards[shard_id] = detalles_shards.get(shard_id, 0) + 1

        duracion_ms = int((time.monotonic() - tiempo_inicio) * 1000)

        return {
            "stream": nombre_stream,
            "total_pedidos": len(pedidos),
            "enviados_exitosos": total_enviados,
            "fallidos": total_fallidos,
            "tiempo_ms": duracion_ms,
            "tasa_pedidos_segundo": round((total_enviados / (duracion_ms / 1000.0)), 2) if duracion_ms > 0 else 0,
            "distribucion_shards": detalles_shards,
            "modo": "AWS_REAL" if self.es_real else "SIMULADO_LOCAL",
        }

    def obtener_todos_los_shards(self, nombre_stream: str = STREAM_NAME_DEFAULT) -> list[str]:
        desc = self.describir_stream(nombre_stream)
        return [s["ShardId"] for s in desc["Shards"]]

    def leer_lote(
        self,
        shard_id: str,
        shard_iterator: str | None = None,
        nombre_stream: str = STREAM_NAME_DEFAULT,
        tipo_iterador: str = "TRIM_HORIZON",
        limite: int = 100,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Lee un lote de registros desde un shard específico."""
        if not shard_iterator:
            if self.es_real:
                res_it = self._cliente_boto.get_shard_iterator(
                    StreamName=nombre_stream,
                    ShardId=shard_id,
                    ShardIteratorType=tipo_iterador,
                )
                shard_iterator = res_it["ShardIterator"]
            else:
                shard_iterator = self._simulador.get_shard_iterator(shard_id, iterator_type=tipo_iterador)

        if self.es_real:
            res_rec = self._cliente_boto.get_records(
                ShardIterator=shard_iterator,
                Limit=limite,
            )
        else:
            res_rec = self._simulador.get_records(shard_iterator=shard_iterator, limit=limite)

        raw_records = res_rec.get("Records", [])
        next_iterator = res_rec.get("NextShardIterator")

        pedidos_parseados = []
        for r in raw_records:
            data = r["Data"]
            if isinstance(data, str):
                # En algunos entornos moto/boto data puede venir en base64
                data = base64.b64decode(data)
            try:
                pedido_dict = json.loads(data.decode("utf-8"))
                pedidos_parseados.append({
                    "seq": r.get("SequenceNumber"),
                    "partition_key": r.get("PartitionKey"),
                    "pedido": pedido_dict,
                })
            except Exception as e:
                log.error(f"Error decodificando registro de Kinesis: {e}")

        return pedidos_parseados, next_iterator
