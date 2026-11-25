@echo off
chcp 65001 > nul
echo ========================================================================
echo   DISPARANDO TRASPASO MASIVO DE PEDIDOS DE DELIVERY A AMAZON KINESIS
echo ========================================================================
echo.
.venv\Scripts\python generador_mock_delivery/simulador_traspaso.py --cantidad 100 %*
pause
