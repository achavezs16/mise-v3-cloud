@echo off
chcp 65001 > nul
echo ========================================================================
echo       EJECUTANDO DEMOSTRACION AMAZON KINESIS DATA STREAMS - MISE v3
echo ========================================================================
echo.
.venv\Scripts\python cli_kinesis.py demo %*
pause
