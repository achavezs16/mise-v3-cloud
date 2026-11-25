"""Pruebas de integración del flujo delivery de MISE v3."""

import base64
import json

from funciones.utiles import cargar
from pos_dominio import claves
from pos_dominio.infra.dynamodb import AlmacenDynamo


def evento_kinesis(pedidos):
    return {
        "Records": [
            {
                "kinesis": {
                    "sequenceNumber": str(i + 1),
                    "data": base64.b64encode(
                        json.dumps(pedido).encode("utf-8")
                    ).decode("ascii"),
                }
            }
            for i, pedido in enumerate(pedidos)
        ]
    }


def test_generacion_lote(nube):
    productor = cargar("pedidos-externos")

    pedidos = [
        productor._generar_pedido_mock("demo", "local_01")
        for _ in range(50)
    ]

    assert len({p["pedido_id"] for p in pedidos}) == 50

    for pedido in pedidos:
        assert pedido["total"] == sum(
            i["subtotal"] for i in pedido["items"]
        )
        assert pedido["subtotal"] == pedido["total"]

        for item in pedido["items"]:
            assert item["subtotal"] == (
                item["cantidad"] * item["precio_unitario"]
            )


def test_transferencia_y_persistencia(nube):
    productor = cargar("pedidos-externos")
    consumidor = cargar("consumidor-delivery")

    pedidos = [
        productor._generar_pedido_mock("demo", "local_01")
        for _ in range(10)
    ]

    resultado = consumidor.lambda_handler(
        evento_kinesis(pedidos), None
    )

    assert resultado["batchItemFailures"] == []

    almacen = AlmacenDynamo(nube.dynamo)
    L = claves.local("demo", "local_01")

    for original in pedidos:
        guardado = almacen.pedido(L, original["pedido_id"])

        assert guardado is not None
        assert guardado.total == original["total"]
        assert guardado.estado == "abierto"
        assert guardado.n_items == len(original["items"])
        assert len(guardado.items) == len(original["items"])
        assert sum(i.subtotal for i in guardado.items) == guardado.total


def test_idempotencia_delivery(nube):
    productor = cargar("pedidos-externos")
    consumidor = cargar("consumidor-delivery")

    pedido = productor._generar_pedido_mock("demo", "local_01")
    evento = evento_kinesis([pedido])

    primero = consumidor.lambda_handler(evento, None)
    assert primero["batchItemFailures"] == []

    antes = nube.dynamo.scan(
        TableName="operacion", Select="COUNT"
    )["Count"]

    segundo = consumidor.lambda_handler(evento, None)
    assert segundo["batchItemFailures"] == []

    despues = nube.dynamo.scan(
        TableName="operacion", Select="COUNT"
    )["Count"]

    assert antes == despues


def test_fallo_parcial_lote(nube):
    productor = cargar("pedidos-externos")
    consumidor = cargar("consumidor-delivery")

    valido = productor._generar_pedido_mock("demo", "local_01")
    invalido = {"tenant_id": "demo"}

    resultado = consumidor.lambda_handler(
        evento_kinesis([valido, invalido]), None
    )

    assert resultado["batchItemFailures"] == [
        {"itemIdentifier": "2"}
    ]


class KinesisSimulado:

    def __init__(self):
        self.registros = []

    def describe_stream_summary(self, **kwargs):
        return {
            "StreamDescriptionSummary": {
                "StreamName": "pos-delivery-stream",
                "StreamStatus": "ACTIVE",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/pos-delivery-stream",
                "OpenShardCount": 2,
                "RetentionPeriodHours": 24,
            }
        }

    def put_records(self, **kwargs):
        registros = kwargs["Records"]
        self.registros.extend(registros)
        return {
            "FailedRecordCount": 0,
            "Records": [
                {
                    "SequenceNumber": str(i + 1),
                    "ShardId": "shardId-000000000000",
                }
                for i in range(len(registros))
            ],
        }


def preparar_productor(monkeypatch):
    from pos_dominio.infra import aws

    monkeypatch.setenv("STREAM_DELIVERY_ACTIVO", "1")

    productor = cargar("pedidos-externos")
    kinesis = KinesisSimulado()
    cliente_original = aws.cliente

    monkeypatch.setattr(
        aws,
        "cliente",
        lambda servicio: (
            kinesis if servicio == "kinesis"
            else cliente_original(servicio)
        ),
    )

    return productor.lambda_handler, kinesis


def test_http_estado_delivery(nube, monkeypatch):
    from funciones.utiles import evento

    funcion, _ = preparar_productor(monkeypatch)

    respuesta = funcion(evento(
        "GET /locales/{local_id}/delivery/importar",
        {"local_id": "local_01"},
        sub="admin1",
    ))

    assert respuesta["statusCode"] == 200, respuesta


def test_http_importacion_delivery(nube, monkeypatch):
    from funciones.utiles import evento

    funcion, kinesis = preparar_productor(monkeypatch)

    respuesta = funcion(evento(
        "POST /locales/{local_id}/delivery/importar",
        {"local_id": "local_01"},
        {"cantidad": 50},
        sub="admin1",
    ))

    assert respuesta["statusCode"] in (200, 201, 202), respuesta
    assert len(kinesis.registros) == 50

    for registro in kinesis.registros:
        pedido = json.loads(registro["Data"])

        assert pedido["tenant_id"] == "demo"
        assert pedido["local_id"] == "local_01"
        assert registro["PartitionKey"] == "demo#local_01"


def test_http_delivery_rechaza_garzon(nube, monkeypatch):
    from funciones.utiles import evento

    funcion, kinesis = preparar_productor(monkeypatch)

    for metodo in ("GET", "POST"):
        respuesta = funcion(evento(
            f"{metodo} /locales/{{local_id}}/delivery/importar",
            {"local_id": "local_01"},
            {"cantidad": 10} if metodo == "POST" else None,
            sub="garzon1",
        ))

        assert respuesta["statusCode"] == 403, respuesta

    assert not kinesis.registros


def test_http_delivery_sin_autenticacion(nube, monkeypatch):
    from funciones.utiles import evento

    funcion, kinesis = preparar_productor(monkeypatch)

    respuesta = funcion(evento(
        "POST /locales/{local_id}/delivery/importar",
        {"local_id": "local_01"},
        {"cantidad": 10},
        sub=None,
    ))

    assert respuesta["statusCode"] == 401, respuesta
    assert not kinesis.registros


def test_http_delivery_otro_local(nube, monkeypatch):
    from funciones.utiles import evento

    funcion, kinesis = preparar_productor(monkeypatch)

    respuesta = funcion(evento(
        "POST /locales/{local_id}/delivery/importar",
        {"local_id": "otro_local"},
        {"cantidad": 10},
        sub="admin1",
    ))

    assert respuesta["statusCode"] == 403, respuesta
    assert not kinesis.registros


def test_webhook_kinesis_activo(nube, monkeypatch):
    import hashlib
    import hmac
    from funciones.utiles import evento

    funcion, kinesis = preparar_productor(monkeypatch)

    def put_record(**kwargs):
        kinesis.registros.append(kwargs)
        return {
            "SequenceNumber": "1",
            "ShardId": "shardId-000000000000",
        }

    monkeypatch.setattr(
        kinesis, "put_record", put_record, raising=False
    )

    datos = b'{"pedido": 1}'
    firma = hmac.new(
        b"secreto-de-prueba",
        datos,
        hashlib.sha256,
    ).hexdigest()

    respuesta = funcion(evento(
        "POST /webhooks/{plataforma}/{tenant_id}/{local_id}",
        {
            "plataforma": "demo",
            "tenant_id": "demo",
            "local_id": "local_01",
        },
        datos,
        sub=None,
        command_id=None,
        encabezados={"x-firma": firma},
    ))

    assert respuesta["statusCode"] == 202, respuesta
    assert len(kinesis.registros) == 1

    registro = kinesis.registros[0]
    pedido = json.loads(registro["Data"])

    assert registro["PartitionKey"] == "demo#local_01"
    assert pedido["tenant_id"] == "demo"
    assert pedido["local_id"] == "local_01"
    assert pedido["pedido_id"]


def test_http_importacion_fallos_parciales(nube, monkeypatch):
    from funciones.utiles import evento

    funcion, kinesis = preparar_productor(monkeypatch)

    def put_records(**kwargs):
        registros = kwargs["Records"]
        return {
            "FailedRecordCount": 1,
            "Records": [
                {"ErrorCode": "ProvisionedThroughputExceededException"}
            ] + [
                {"SequenceNumber": str(i)}
                for i in range(1, len(registros))
            ],
        }

    monkeypatch.setattr(kinesis, "put_records", put_records)

    respuesta = funcion(evento(
        "POST /locales/{local_id}/delivery/importar",
        {"local_id": "local_01"},
        {"cantidad": 50},
        sub="admin1",
    ))

    assert respuesta["statusCode"] == 200, respuesta

    datos = json.loads(respuesta["body"])

    assert datos["cantidad_solicitada"] == 50
    assert datos["enviados"] == 48
    assert datos["fallidos"] == 2


def test_http_importacion_500_pedidos(nube, monkeypatch):
    from funciones.utiles import evento

    funcion, kinesis = preparar_productor(monkeypatch)

    respuesta = funcion(evento(
        "POST /locales/{local_id}/delivery/importar",
        {"local_id": "local_01"},
        {"cantidad": 500},
        sub="admin1",
    ))

    assert respuesta["statusCode"] == 200, respuesta

    datos = json.loads(respuesta["body"])

    assert datos["enviados"] == 500
    assert datos["fallidos"] == 0
    assert len(kinesis.registros) == 500


def test_evento_delivery_compatible_con_procesador(nube):
    import gzip
    from pos_dominio import efectos
    from boto3.dynamodb.types import TypeDeserializer

    productor = cargar("pedidos-externos")
    consumidor = cargar("consumidor-delivery")
    procesador = cargar("procesador-eventos")

    pedido = productor._generar_pedido_mock("demo", "local_01")

    resultado = consumidor.lambda_handler(
        evento_kinesis([pedido]), None
    )

    assert resultado["batchItemFailures"] == []

    L = claves.local("demo", "local_01")

    registro = nube.dynamo.get_item(
        TableName="operacion",
        Key={
            "PK": {"S": claves.eventos(L)},
            "SK": {"S": claves.sk_evento(pedido["event_id"])},
        },
    )["Item"]

    deserializador = TypeDeserializer()

    evento = {
        k: deserializador.deserialize(v)
        for k, v in registro.items()
    }

    assert evento["event_id"] == pedido["event_id"]
    assert evento["tipo"] == "delivery.ingresado"
    assert evento["origen"] == "delivery"
    assert evento["expira_en"] > 0

    assert efectos.agregados(evento) == []
    assert efectos.avisos(evento) == []
    assert efectos.mensaje_dte(evento) is None

    resultado = procesador.lambda_handler({
        "Records": [{
            "eventName": "INSERT",
            "dynamodb": {
                "SequenceNumber": "1",
                "NewImage": registro,
            },
        }],
    }, None)

    assert resultado["batchItemFailures"] == []

    objeto = nube.s3.get_object(
        Bucket=nube.bucket,
        Key=efectos.clave_archivo(evento),
    )

    archivado = json.loads(
        gzip.decompress(objeto["Body"].read())
    )

    assert archivado["event_id"] == pedido["event_id"]
    assert archivado["tipo"] == "delivery.ingresado"
