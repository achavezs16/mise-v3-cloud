@echo off
chcp 65001 > nul
echo ========================================================================
echo   INICIANDO PROYECTO 2: API MOCK DELIVERY & TRASPASO A KINESIS
echo   Swagger UI disponible en: http://localhost:8000/docs
echo ========================================================================
echo.
.venv\Scripts\python -m uvicorn generador_mock_delivery.app_api:app --port 8000 --reload
pause
