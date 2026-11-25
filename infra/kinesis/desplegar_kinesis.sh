#!/usr/bin/env bash
# Despliegue de Amazon Kinesis Data Streams para Delivery en AWS CloudShell / Learner Lab.
#
# Uso en CloudShell:
#   bash infra/kinesis/desplegar_kinesis.sh
#
# Requiere:
#   - AWS CLI configurado (o sesión activa en CloudShell)
#   - Rol IAM LabRole (o equivalente en tu cuenta)

set -euo pipefail
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_PAGER=""

STREAM_NAME="pos-delivery-stream"
SHARDS_COUNT=2
DLQ_NAME="pos-delivery-fallidos"
LAMBDA_NAME="consumidor-delivery-kinesis"

paso()  { echo; echo "==> $*"; }
ok()    { echo "    ✔ $*"; }
aviso() { echo "    ! $*"; }

CUENTA=$(aws sts get-caller-identity --query Account --output text)
ROL="arn:aws:iam::${CUENTA}:role/LabRole"

echo "========================================================================"
echo " Despliegue de Kinesis Data Streams para Delivery Masivo (MISE POS v3)"
echo " Cuenta: $CUENTA | Región: $AWS_DEFAULT_REGION | Rol: $ROL"
echo "========================================================================"

# ---------------------------------------------------------------------------------------------------
paso "1. Creando Dead Letter Queue (DLQ) en SQS"
if aws sqs get-queue-url --queue-name "$DLQ_NAME" >/dev/null 2>&1; then
    ok "Cola DLQ '$DLQ_NAME' ya existe."
    DLQ_URL=$(aws sqs get-queue-url --queue-name "$DLQ_NAME" --query QueueUrl --output text)
else
    DLQ_URL=$(aws sqs create-queue --queue-name "$DLQ_NAME" \
        --attributes MessageRetentionPeriod=1209600 \
        --query QueueUrl --output text)
    ok "Cola DLQ '$DLQ_NAME' creada ($DLQ_URL)."
fi
DLQ_ARN=$(aws sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

# ---------------------------------------------------------------------------------------------------
paso "2. Creando Amazon Kinesis Data Stream ('$STREAM_NAME')"
if aws kinesis describe-stream-summary --stream-name "$STREAM_NAME" >/dev/null 2>&1; then
    ok "Stream '$STREAM_NAME' ya existe."
else
    aws kinesis create-stream \
        --stream-name "$STREAM_NAME" \
        --shard-count "$SHARDS_COUNT" \
        --stream-mode-details StreamMode=PROVISIONED
    ok "Stream '$STREAM_NAME' creado con $SHARDS_COUNT shards."
fi

# Esperar a que el stream esté ACTIVE
echo "    Esperando a que el stream esté ACTIVE..."
aws kinesis wait stream-exists --stream-name "$STREAM_NAME"
STREAM_ARN=$(aws kinesis describe-stream-summary --stream-name "$STREAM_NAME" --query 'StreamDescriptionSummary.StreamARN' --output text)
ok "Stream listo y activo: $STREAM_ARN"

# ---------------------------------------------------------------------------------------------------
paso "3. Event Source Mapping (Disparador Kinesis -> Lambda)"
# Buscar si ya existe el mapeo
EXISTENTE_MAPPING=$(aws lambda list-event-source-mappings \
    --event-source-arn "$STREAM_ARN" \
    --function-name "$LAMBDA_NAME" \
    --query 'EventSourceMappings[0].UUID' \
    --output text 2>/dev/null || echo "None")

if [ "$EXISTENTE_MAPPING" != "None" ] && [ "$EXISTENTE_MAPPING" != "" ]; then
    ok "Event Source Mapping ya existe (UUID: $EXISTENTE_MAPPING)."
else
    if aws lambda get-function --function-name "$LAMBDA_NAME" >/dev/null 2>&1; then
        aws lambda create-event-source-mapping \
            --function-name "$LAMBDA_NAME" \
            --event-source-arn "$STREAM_ARN" \
            --batch-size 100 \
            --maximum-batching-window-in-seconds 2 \
            --starting-position LATEST \
            --bisect-batch-on-function-error \
            --maximum-retry-attempts 3 \
            --destination-config "OnFailure={Destination=$DLQ_ARN}" \
            --function-response-types ReportBatchItemFailures >/dev/null
        ok "Event Source Mapping creado exitosamente."
    else
        aviso "La función Lambda '$LAMBDA_NAME' aún no está creada en AWS. Puedes crearla con el código de kinesis_delivery."
    fi
fi

echo
echo "========================================================================"
echo " ✔ Despliegue de infraestructura Kinesis completado exitosamente."
echo "   Stream ARN: $STREAM_ARN"
echo "   DLQ ARN:    $DLQ_ARN"
echo "========================================================================"
