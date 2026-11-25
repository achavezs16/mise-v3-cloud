"""Generador de pedidos mock de plataformas de delivery (Uber Eats, Rappi, PedidosYa).

Simula datos realistas de restaurantes chilenos, incluyendo clientes, direcciones,
repartidores, productos del catálogo, notas de preparación, propinas y subtotales.
"""

from __future__ import annotations

import random
import secrets
import time
from datetime import datetime, timezone
from typing import Any

_ALFABETO = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generar_ulid() -> str:
    """Genera un identificador ULID de 26 caracteres ordenable cronológicamente."""
    timestamp_ms = time.time_ns() // 1_000_000
    tiempo_chars = "".join(_ALFABETO[(timestamp_ms >> (5 * i)) & 31] for i in reversed(range(10)))
    random_chars = "".join(_ALFABETO[(secrets.randbits(80) >> (5 * i)) & 31] for i in reversed(range(16)))
    return tiempo_chars + random_chars


PLATAFORMAS = ["ubereats", "rappi", "pedidosya"]

CLIENTES_MOCK = [
    ("Francisca Morales", "+56987654321", "Av. Providencia 1234, Depto 502, Providencia"),
    ("Sebastián Valenzuela", "+56976543210", "Los Leones 234, Casa B, Providencia"),
    ("Valentina Silva", "+56965432109", "Av. Apoquindo 4500, Oficina 801, Las Condes"),
    ("Diego Muñoz", "+56954321098", "Manuel Montt 890, Depto 12, Ñuñoa"),
    ("Camila Rojas", "+56943210987", "Irarrázaval 3200, Depto 1104, Ñuñoa"),
    ("Matías Contreras", "+56932109876", "Av. Vitacura 5600, Vitacura"),
    ("Constanza Araya", "+56921098765", "Santa Beatriz 111, Depto 303, Providencia"),
    ("Nicolás Castro", "+56910987654", "General Holley 145, Depto 702, Providencia"),
]

REPARTIDORES_MOCK = [
    ("Carlos Gómez", "Moto Yamaha Negra - Patente AB-1234"),
    ("Andrés Pérez", "Bicicleta Eléctrica"),
    ("Luis Torres", "Moto Honda Roja - Patente CD-5678"),
    ("Gabriel Fernández", "Moto Suzuki Azul - Patente EF-9012"),
    ("Jorge Méndez", "Bicicleta"),
]

PRODUCTOS_CATALOGO = [
    ("BURGER_CLASICA", "Hamburguesa Clásica", 7900, ["Sin cebolla", "Término medio", "Extra salsa"]),
    ("BURGER_DOBLE", "Hamburguesa Doble Queso Tocino", 9900, ["Papas extra crujientes", "Sin pepinillos"]),
    ("BURGER_VEGGIE", "Hamburguesa NotBurger Palta", 8900, ["Pan vegano", "Sin mayonesa"]),
    ("PIZZA_PEPPERONI", "Pizza Pepperoni Individual", 8500, ["Masa a la piedra", "Extra queso"]),
    ("PIZZA_MARGARITA", "Pizza Margarita", 7500, ["Albahaca fresca"]),
    ("PAPAS_RUSTICAS", "Papas Rústicas con Cheddar", 4500, ["Cheddar y tocino"]),
    ("EMPANADAS_QUESO", "Empanadas de Queso (3 un)", 3900, []),
    ("BEBIDA_COCA_REG", "Bebida Coca-Cola 350ml", 1800, []),
    ("BEBIDA_COCA_ZERO", "Bebida Coca-Cola Zero 350ml", 1800, []),
    ("CERVEZA_IPA", "Cerveza Artesanal IPA 473ml", 3900, []),
    ("JUGO_NATURAL", "Jugo Natural de Frambuesa", 3200, ["Sin azúcar"]),
]


def generar_pedido_delivery_mock(
    tenant_id: str = "demo",
    local_id: str = "local_01",
    plataforma: str | None = None,
) -> dict[str, Any]:
    """Genera un pedido sintético de delivery con formato estructurado."""
    plataforma_sel = plataforma if plataforma in PLATAFORMAS else random.choice(PLATAFORMAS)
    cliente = random.choice(CLIENTES_MOCK)
    repartidor = random.choice(REPARTIDORES_MOCK)

    # Seleccionar 1 a 4 items aleatorios del catálogo
    num_items = random.randint(1, 4)
    productos_sel = random.sample(PRODUCTOS_CATALOGO, k=min(num_items, len(PRODUCTOS_CATALOGO)))

    items_pedido = []
    subtotal = 0

    for prod_id, nombre, precio, notas_posibles in productos_sel:
        cantidad = random.randint(1, 2)
        nota = random.choice(notas_posibles) if notas_posibles and random.random() > 0.5 else ""
        subtotal_item = precio * cantidad
        subtotal += subtotal_item

        items_pedido.append({
            "item_id": generar_ulid(),
            "producto_id": prod_id,
            "nombre": nombre,
            "precio_unitario": precio,
            "cantidad": cantidad,
            "subtotal": subtotal_item,
            "notas": nota,
            "estado": "en_espera",
        })

    propina = int(subtotal * 0.10) if random.random() > 0.3 else 0
    total = subtotal + propina

    orden_externa_id = f"{plataforma_sel.upper()}-{random.randint(100000, 999999)}"
    pedido_id = generar_ulid()
    command_id = generar_ulid()
    event_id = generar_ulid()
    ahora_iso = datetime.now(timezone.utc).isoformat()

    return {
        "event_id": event_id,
        "command_id": command_id,
        "pedido_id": pedido_id,
        "tenant_id": tenant_id,
        "local_id": local_id,
        "plataforma": plataforma_sel,
        "orden_externa_id": orden_externa_id,
        "tipo_pedido": "delivery",
        "recibido_en": ahora_iso,
        "cliente": {
            "nombre": cliente[0],
            "telefono": cliente[1],
            "direccion": cliente[2],
        },
        "repartidor": {
            "nombre": repartidor[0],
            "vehiculo": repartidor[1],
        },
        "items": items_pedido,
        "subtotal": subtotal,
        "propina": propina,
        "total": total,
        "metodo_pago": "online_platform",
        "estado": "abierto",
    }


def generar_lote_delivery_mock(
    cantidad: int = 10,
    tenant_id: str = "demo",
    local_id: str = "local_01",
    plataforma: str | None = None,
) -> list[dict[str, Any]]:
    """Genera una lista de N pedidos mock de delivery para cargas masivas."""
    return [
        generar_pedido_delivery_mock(tenant_id=tenant_id, local_id=local_id, plataforma=plataforma)
        for _ in range(cantidad)
    ]
