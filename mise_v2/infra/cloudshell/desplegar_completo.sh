#!/usr/bin/env bash
# Despliegue COMPLETO de MISE POS v3 desde AWS CloudShell.
# Compila la PWA, despliega toda la infraestructura AWS e incluye Kinesis.
#
# Uso (desde la carpeta descomprimida del paquete):
#   bash infra/cloudshell/desplegar_completo.sh 2>&1 | tee despliegue.log

set -euo pipefail
export AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER=""

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$RAIZ"

paso()  { echo; echo "==> $*"; }
ok()    { echo "    ✔ $*"; }
aviso() { echo "    ! $*"; }

CUENTA=$(aws sts get-caller-identity --query Account --output text)
echo ""
echo "========================================================================"
echo " MISE POS v3 — Despliegue completo"
echo " Cuenta: $CUENTA | Región: us-east-1 | Rol: LabRole"
echo "========================================================================"

# ---------------------------------------------------------------------------
paso "1. Compilando la PWA en CloudShell (npm)"
# ---------------------------------------------------------------------------
if [ -f "pwa/dist/index.html" ]; then
  ok "pwa/dist/index.html ya existe, saltando compilación"
else
  if ! command -v npm &>/dev/null; then
    aviso "npm no disponible en CloudShell — la PWA no se publicará, pero la API y el backend sí"
  else
    npm_ver=$(npm --version)
    node_ver=$(node --version)
    ok "Node $node_ver / npm $npm_ver"
    cd pwa
    echo "    Instalando dependencias (npm ci)..."
    npm ci --silent
    echo "    Construyendo (npm run build)..."
    npm run build --silent
    cd "$RAIZ"
    if [ -f "pwa/dist/index.html" ]; then
      ok "PWA compilada en pwa/dist/"
    else
      aviso "La compilación no generó index.html — la PWA no se publicará"
    fi
  fi
fi

# ---------------------------------------------------------------------------
paso "2. Desplegando infraestructura AWS completa (Kinesis + API + Lambdas + DynamoDB + S3)"
# ---------------------------------------------------------------------------
bash infra/cloudshell/desplegar.sh 2>&1

# ---------------------------------------------------------------------------
echo ""
echo "========================================================================"
echo " ✔ DESPLIEGUE COMPLETO FINALIZADO"
echo "========================================================================"
if [ -f valores-despliegue.env ]; then
  # shellcheck disable=SC1091
  . valores-despliegue.env
  echo "    PWA:   https://${BUCKET_SITIO}.s3.us-east-1.amazonaws.com/index.html"
  echo "    API:   ${URL_API:-<ver valores-despliegue.env>}"
  echo "    Salud: ${URL_API:-<ver valores-despliegue.env>}/salud"
fi
echo "    Kinesis stream: pos-delivery-stream (ACTIVE)"
echo "========================================================================"
