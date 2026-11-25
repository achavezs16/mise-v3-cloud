# Guía de Demostración: Pipeline de Delivery con Amazon Kinesis Data Streams (MISE POS v3)

Esta guía explica la arquitectura, los componentes y los comandos para demostrar el flujo completo de streaming e ingesta masiva:

$$\text{Datos Mock} \longrightarrow \text{API REST (FastAPI)} \longrightarrow \text{Amazon Kinesis Data Streams} \longrightarrow \text{BD Sistema MISE (DynamoDB)}$$

---

## 1. Arquitectura del Flujo

1. **Datos Mock (`kinesis_delivery/mock_data.py`):**
   - Genera pedidos realistas de plataformas de delivery (**Uber Eats, Rappi, PedidosYa**).
   - Incluye clientes chilenos, direcciones, repartidores, productos del catálogo con notas, cálculo de subtotales, propina y total en CLP, asignando identificadores únicos ULID.

2. **API REST (`kinesis_delivery/api.py`):**
   - Desarrollada en **FastAPI** con documentación interactiva Swagger UI en `/docs`.
   - Expone endpoints para recibir pedidos externos, disparar ráfagas batch masivas, monitorear el estado del stream de Kinesis y consultar la base de datos de MISE.

3. **Amazon Kinesis Data Streams (`kinesis_delivery/kinesis_engine.py`):**
   - Utiliza `boto3` para interactuar con Amazon Kinesis.
   - Envía registros mediante la API de alto rendimiento `PutRecords` (en lotes de hasta 500 registros por llamada).
   - Utiliza como **PartitionKey** `tenant_id#local_id` para garantizar que los pedidos de un mismo restaurante permanezcan estrictamente en orden secuencial por shard.
   - **Modo Dual Inteligente:** Si detecta credenciales de AWS activas (por ejemplo en AWS CloudShell o con `aws configure`), se conecta al servicio real de AWS. Si se ejecuta en local sin credenciales, utiliza automáticamente el motor simulador en memoria con la misma interfaz exacta (shards, iteradores, límites y secuencias).

4. **Base de Datos Operacional de MISE (`kinesis_delivery/mise_db.py`):**
   - Implementa el diseño de tabla única (*Single-Table Design*) de DynamoDB para MISE POS v2/v3:
     - `L#PED#<pedido_id>` / `PEDIDO`: Cabecera del pedido (`tipo_pedido = delivery`, datos de cliente y repartidor).
     - `L#PED#<pedido_id>` / `ITEM#<item_id>`: Cada producto individual de la orden.
     - `L#EXTERNO#<plataforma>` / `ORDEN#<orden_id>`: Índice de búsqueda rápida y de-duplicación.
     - `L#CMD#<command_id>` / `CMD`: Registro de idempotencia del comando.
     - `L#EVT` / `EVT#<event_id>`: Evento de auditoría Outbox (`tipo = delivery.ingresado`).
     - `L#IMP#cocina` / `PEND#<trabajo_id>`: Ticket de comanda enviado a la cocina.

5. **Consumidor Kinesis (`kinesis_delivery/consumer.py`):**
   - Lee micro-lotes desde los shards de Kinesis mediante iteradores `TRIM_HORIZON` / `LATEST`.
   - Ejecuta la transacción atómica hacia la base de datos de MISE, registrando métricas y descartando duplicados.

---

## 2. Comando Rápido: Demostración Completa de Punta a Punta

Para demostrar que sabes utilizar Kinesis y presentar el pipeline funcionando de principio a fin en la terminal:

```powershell
.venv\Scripts\python cli_kinesis.py demo --cantidad 25
```

Este comando ejecuta automáticamente los 4 pasos:
1. Provisiona/Verifica el stream de Kinesis (`pos-delivery-stream`) con 2 shards.
2. Genera 25 pedidos mock de UberEats, Rappi y PedidosYa.
3. Transmite los pedidos en batch a Kinesis usando `PutRecords`.
4. El consumidor extrae los registros de los shards y aplica las transacciones atómicas en la BD de MISE.
5. Imprime un resumen de entidades y la tabla formateada con los pedidos creados en la base de datos.

---

## 3. Demostración Interactiva con la API Web (Swagger UI)

Para levantar la API REST y probar el flujo desde el navegador:

```powershell
.venv\Scripts\python cli_kinesis.py iniciar-api --puerto 8000
```

1. Abre tu navegador en: [http://localhost:8000/docs](http://localhost:8000/docs)
2. Verás los endpoints listos para interactuar:
   - `POST /api/kinesis/crear-stream`: Crea el stream en Kinesis.
   - `GET /api/kinesis/estado`: Muestra el ARN, estado `ACTIVE` y los shards.
   - `POST /api/delivery/simular-batch`: Envía una ráfaga de pedidos mock a Kinesis (ej. 50 pedidos) y los procesa en la BD de MISE.
   - `GET /api/mise/pedidos`: Lista los pedidos que han llegado a la base de datos.
   - `GET /api/mise/estadisticas`: Muestra el total de pedidos, items, eventos outbox y monto recaudado en CLP.

---

## 4. Comandos Individuales de la CLI

### Crear o Verificar el Stream en Kinesis
```powershell
.venv\Scripts\python cli_kinesis.py crear-stream --shards 2
```

### Simular y Transmitir Pedidos Mock Batch a Kinesis
```powershell
.venv\Scripts\python cli_kinesis.py simular-batch --cantidad 50 --plataforma ubereats
```

### Consumir Manualmente los Registros de Kinesis hacia la BD
```powershell
.venv\Scripts\python cli_kinesis.py consumir --limite 100
```

### Inspeccionar la Base de Datos de MISE
```powershell
.venv\Scripts\python cli_kinesis.py ver-bd --limite 15
```

---

## 5. Verificación, Conexión y Búsqueda de Datos en AWS Real

Para conectar con AWS real, verificar que el stream reciba datos y **comprobar que el sistema va a buscar los datos**:

```powershell
.venv\Scripts\python cli_kinesis.py verificar-aws --stream pos-delivery-stream --region us-east-1
```
*(O simplemente ejecutando `ejecutar_verificar_aws.bat`)*

### Flujo de Verificación en AWS:
1. **Identidad STS:** Conecta con AWS STS (`get_caller_identity`) y valida tu cuenta y rol (`LabRole`).
2. **Kinesis Activo:** Conecta con Amazon Kinesis (`describe_stream_summary`), confirmando estado `ACTIVE`, ARN y shards abiertos.
3. **Inyección en Streaming:** Envía un pedido real a Kinesis vía `put_record`, obteniendo su `SequenceNumber` y `ShardId`.
4. **Búsqueda de Datos en Kinesis:** Se conecta al shard correspondiente mediante `get_shard_iterator` y `get_records`, **yendo a buscar activamente el pedido al stream** y mostrando los datos recuperados en la terminal.
5. **Búsqueda de Datos en DynamoDB:** Consulta la tabla `operacion` de DynamoDB (`L#PED#...`) para verificar que el pedido quedó persistido transaccionalmente por el consumidor.

---

## 6. Despliegue en AWS CloudShell (Learner Lab)

1. En tu máquina genera el paquete zip para CloudShell:
   ```powershell
   .venv\Scripts\python mise_v2/scripts/paquete_cloudshell.py
   ```
2. En AWS CloudShell (us-east-1), sube `dist/pos-cloudshell.zip` (Actions > Upload file).
3. Descomprime y ejecuta el despliegue de Kinesis y el POS:
   ```bash
   unzip -o pos-cloudshell.zip -d pos && cd pos
   bash infra/kinesis/desplegar_kinesis.sh
   bash infra/cloudshell/desplegar.sh
   ```
El script creará automáticamente el stream `pos-delivery-stream`, la cola DLQ en SQS, la función Lambda `consumidor-delivery-kinesis` y el `EventSourceMapping` con división de lote ante errores (`BisectBatchOnFunctionError`) y reporte de fallos parciales (`ReportBatchItemFailures`).
