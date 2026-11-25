"""Adaptador de Base de Datos para el sistema MISE POS.

Implementa el modelo transaccional de DynamoDB (tabla `operacion`) siguiendo
las convenciones y diseño de clave de MISE v2 (docs/migracion/01-modelo-dynamodb.md):
- Pedido: L#PED#<pedido_id> / PEDIDO (tipo_pedido="delivery")
- Ítems: L#PED#<pedido_id> / ITEM#<item_id>
- Índice externo: L#EXTERNO#<plataforma> / ORDEN#<orden_externa_id>
- Idempotencia: L#CMD#<command_id> / CMD
- Evento Outbox: L#EVT / EVT#<event_id> (tipo="delivery.ingresado")
- Ticket de Cocina: L#IMP#cocina / PEND#<trabajo_id>
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger("mise.db")

ARCHIVO_BD_LOCAL = Path(__file__).resolve().parent / "bd_mise_almacen.json"


class AlmacenMiseDB:
    """Base de datos operacional de MISE POS con soporte transaccional y persistencia."""

    def __init__(self, archivo_persistencia: Path | None = None) -> None:
        self.archivo_persistencia = archivo_persistencia or ARCHIVO_BD_LOCAL
        self._lock = threading.Lock()
        # Estructura { (PK, SK): item_dict }
        self.tabla_operacion: dict[str, dict[str, Any]] = {}
        self.cargar()

    def _clave(self, pk: str, sk: str) -> str:
        return f"{pk}::{sk}"

    def cargar(self) -> None:
        if self.archivo_persistencia.exists():
            try:
                with open(self.archivo_persistencia, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.tabla_operacion = data.get("tabla_operacion", {})
            except Exception as e:
                log.warning(f"No se pudo cargar BD persistente previa: {e}")

    def guardar(self) -> None:
        try:
            with open(self.archivo_persistencia, "w", encoding="utf-8") as f:
                json.dump({"tabla_operacion": self.tabla_operacion}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"Error guardando persistencia en disco: {e}")

    def limpiar(self) -> None:
        """Reinicia la base de datos para pruebas limpias."""
        with self._lock:
            self.tabla_operacion.clear()
            self.guardar()

    def aplicar_transaccion_delivery(self, pedido_dict: dict[str, Any]) -> dict[str, Any]:
        """Aplica la transacción atómica equivalente a TransactWriteItems en DynamoDB."""
        tenant_id = pedido_dict["tenant_id"]
        local_id = pedido_dict["local_id"]
        plataforma = pedido_dict["plataforma"]
        orden_externa_id = pedido_dict["orden_externa_id"]
        pedido_id = pedido_dict["pedido_id"]
        command_id = pedido_dict.get("command_id", f"CMD-{pedido_id}")
        event_id = pedido_dict.get("event_id", f"EVT-{pedido_id}")
        L = f"TENANT#{tenant_id}#LOCAL#{local_id}"

        with self._lock:
            # 1. Comprobar idempotencia por CMD
            pk_cmd = f"{L}#CMD#{command_id}"
            sk_cmd = "CMD"
            clave_cmd = self._clave(pk_cmd, sk_cmd)
            if clave_cmd in self.tabla_operacion:
                log.info(f"[Idempotencia] Comando {command_id} ya fue aplicado anteriormente.")
                return {"resultado": "DUPLICADO_IGNORADO", "pedido_id": pedido_id}

            # 2. Comprobar si la orden externa ya fue registrada
            pk_ext = f"{L}#EXTERNO#{plataforma.upper()}"
            sk_ext = f"ORDEN#{orden_externa_id}"
            clave_ext = self._clave(pk_ext, sk_ext)
            if clave_ext in self.tabla_operacion:
                log.info(f"[Duplicado] Orden externa {orden_externa_id} ya registrada.")
                return {"resultado": "ORDEN_YA_EXISTE", "pedido_id": self.tabla_operacion[clave_ext]["pedido_id"]}

            # 3. Guardar registro CMD
            self.tabla_operacion[clave_cmd] = {
                "PK": pk_cmd,
                "SK": sk_cmd,
                "tipo_item": "CMD",
                "estado": "aplicado",
                "http_status": 200,
                "creado_en": datetime.now(timezone.utc).isoformat(),
            }

            # 4. Guardar índice de orden externa
            self.tabla_operacion[clave_ext] = {
                "PK": pk_ext,
                "SK": sk_ext,
                "tipo_item": "EXTERNO_INDEX",
                "plataforma": plataforma,
                "orden_externa_id": orden_externa_id,
                "pedido_id": pedido_id,
                "total": pedido_dict["total"],
                "recibido_en": pedido_dict["recibido_en"],
            }

            # 5. Guardar entidad cabecera PEDIDO
            pk_ped = f"{L}#PED#{pedido_id}"
            sk_ped = "PEDIDO"
            self.tabla_operacion[self._clave(pk_ped, sk_ped)] = {
                "PK": pk_ped,
                "SK": sk_ped,
                "tipo_item": "PEDIDO",
                "tipo_pedido": "delivery",
                "plataforma": plataforma,
                "orden_externa_id": orden_externa_id,
                "estado": "abierto",
                "version": 1,
                "subtotal": pedido_dict["subtotal"],
                "propina": pedido_dict["propina"],
                "total": pedido_dict["total"],
                "cliente": pedido_dict["cliente"],
                "repartidor": pedido_dict["repartidor"],
                "recibido_en": pedido_dict["recibido_en"],
                "origen": "kinesis.delivery.stream",
            }

            # 6. Guardar cada ITEM del pedido
            for item in pedido_dict.get("items", []):
                item_id = item["item_id"]
                sk_item = f"ITEM#{item_id}"
                self.tabla_operacion[self._clave(pk_ped, sk_item)] = {
                    "PK": pk_ped,
                    "SK": sk_item,
                    "tipo_item": "ITEM",
                    "producto_id": item["producto_id"],
                    "nombre": item["nombre"],
                    "precio_unitario": item["precio_unitario"],
                    "cantidad": item["cantidad"],
                    "subtotal": item["subtotal"],
                    "notas": item.get("notas", ""),
                    "estado": "en_espera",
                }

            # 7. Registrar evento de dominio Outbox
            pk_evt = f"{L}#EVT"
            sk_evt = f"EVT#{event_id}"
            self.tabla_operacion[self._clave(pk_evt, sk_evt)] = {
                "PK": pk_evt,
                "SK": sk_evt,
                "tipo_item": "EVT",
                "tipo": "delivery.ingresado",
                "tenant_id": tenant_id,
                "local_id": local_id,
                "ocurrido_en": pedido_dict["recibido_en"],
                "datos": {
                    "pedido_id": pedido_id,
                    "plataforma": plataforma,
                    "orden_externa_id": orden_externa_id,
                    "total": pedido_dict["total"],
                    "items_count": len(pedido_dict.get("items", [])),
                },
            }

            # 8. Generar ticket de impresión para cocina
            trabajo_id = f"TRAB-{pedido_id[:12]}"
            pk_imp = f"{L}#IMP#cocina"
            sk_imp = f"PEND#{trabajo_id}"
            self.tabla_operacion[self._clave(pk_imp, sk_imp)] = {
                "PK": pk_imp,
                "SK": sk_imp,
                "tipo_item": "PENDIENTE_IMPRESION",
                "tipo_ticket": "comanda_delivery",
                "pedido_id": pedido_id,
                "plataforma": plataforma,
                "creado_en": datetime.now(timezone.utc).isoformat(),
            }

            self.guardar()
            return {"resultado": "EXITO", "pedido_id": pedido_id}

    def listar_pedidos_delivery(self, limite: int = 50) -> list[dict[str, Any]]:
        """Recupera los pedidos de delivery creados en la base de datos."""
        pedidos = []
        with self._lock:
            for clave, item in self.tabla_operacion.items():
                if item.get("tipo_item") == "PEDIDO" and item.get("tipo_pedido") == "delivery":
                    pedidos.append(item)
        pedidos.sort(key=lambda x: x.get("recibido_en", ""), reverse=True)
        return pedidos[:limite]

    def obtener_resumen_estadisticas(self) -> dict[str, Any]:
        """Calcula el conteo de entidades almacenadas en la base de datos."""
        with self._lock:
            pedidos_delivery = sum(1 for item in self.tabla_operacion.values() if item.get("tipo_item") == "PEDIDO" and item.get("tipo_pedido") == "delivery")
            items_totales = sum(1 for item in self.tabla_operacion.values() if item.get("tipo_item") == "ITEM")
            eventos_outbox = sum(1 for item in self.tabla_operacion.values() if item.get("tipo_item") == "EVT")
            tickets_cocina = sum(1 for item in self.tabla_operacion.values() if item.get("tipo_item") == "PENDIENTE_IMPRESION")
            total_recaudado = sum(item.get("total", 0) for item in self.tabla_operacion.values() if item.get("tipo_item") == "PEDIDO" and item.get("tipo_pedido") == "delivery")

            return {
                "pedidos_delivery": pedidos_delivery,
                "items_totales": items_totales,
                "eventos_outbox": eventos_outbox,
                "tickets_cocina_pendientes": tickets_cocina,
                "monto_total_recaudado_clp": total_recaudado,
                "total_items_db": len(self.tabla_operacion),
            }


# Instancia compartida singleton
db_mise = AlmacenMiseDB()
