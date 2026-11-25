@echo off
chcp 65001 > nul
echo ========================================================================
echo       VERIFICACION Y BUSQUEDA DE DATOS EN AWS KINESIS & DYNAMODB
echo ========================================================================
echo.
.venv\Scripts\python cli_kinesis.py verificar-aws %*
pause
