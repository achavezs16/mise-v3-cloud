-- Tabla externa de Amazon Athena para eventos de Delivery procesados desde Kinesis / Firehose.
-- Ubicación en S3 particionada por cliente y fecha.
--
-- Antes de ejecutar: reemplaza <BUCKET_ARCHIVO> por el bucket real (pos-archivo-<cuenta>).
-- Editor de consultas de Athena -> pegar y ejecutar.

CREATE DATABASE IF NOT EXISTS pos;

CREATE EXTERNAL TABLE IF NOT EXISTS pos.delivery_eventos (
  event_id          string,
  pedido_id         string,
  plataforma        string,
  orden_externa_id  string,
  tenant_id         string,
  local_id          string,
  total             bigint,
  subtotal          bigint,
  propina           bigint,
  metodo_pago       string,
  recibido_en       string,
  cliente_nombre    string,
  cliente_telefono  string,
  cliente_direccion string,
  repartidor_nombre string,
  items_count       int
)
PARTITIONED BY (tenant string, fecha date)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<BUCKET_ARCHIVO>/delivery/'
TBLPROPERTIES (
  'projection.enabled'          = 'true',
  'projection.tenant.type'      = 'injected',
  'projection.fecha.type'       = 'date',
  'projection.fecha.format'     = 'yyyy-MM-dd',
  'projection.fecha.range'      = '2026-01-01,NOW',
  'projection.fecha.interval'   = '1',
  'projection.fecha.interval.unit' = 'DAYS',
  'storage.location.template'   = 's3://<BUCKET_ARCHIVO>/delivery/tenant=${tenant}/fecha=${fecha}/'
);

-- Consultas Analíticas de Ejemplo:

-- 1. Total de ventas y ticket promedio por plataforma de delivery (UberEats vs Rappi vs PedidosYa):
-- SELECT plataforma,
--        count(*) AS total_pedidos,
--        sum(total) AS total_recaudado_clp,
--        avg(total) AS ticket_promedio_clp,
--        sum(propina) AS total_propinas_clp
-- FROM pos.delivery_eventos
-- WHERE tenant = 'demo' AND fecha = current_date
-- GROUP BY plataforma
-- ORDER BY total_recaudado_clp DESC;

-- 2. Horas de mayor demanda y ráfagas de pedidos (para dimensionar shards de Kinesis):
-- SELECT substr(recibido_en, 12, 2) AS hora_utc,
--        count(*) AS pedidos_por_hora,
--        count(*) / 60.0 AS promedio_pedidos_por_minuto
-- FROM pos.delivery_eventos
-- WHERE tenant = 'demo' AND fecha >= date_add('day', -7, current_date)
-- GROUP BY substr(recibido_en, 12, 2)
-- ORDER BY hora_utc;
