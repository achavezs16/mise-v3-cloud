@echo off
chcp 65001 > nul
echo ========================================================================
echo       INICIANDO API REST FASTAPI - DELIVERY STREAMING (MISE v3)
echo       Documentacion Swagger: http://localhost:8000/docs
echo ========================================================================
echo.
.venv\Scripts\python cli_kinesis.py iniciar-api %*
pause
