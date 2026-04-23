@echo off
SETLOCAL
TITLE Migrador Web - Full System Starter
COLOR 0B

echo ==========================================
echo    INICIANDO SISTEMA MIGRACAO COMPLETO
echo ==========================================
echo.

:: 1. Limpar processos antigos
echo [1/3] Limpando processos nas portas 3000 e 8080...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :3000 ^| findstr LISTENING') do taskkill /f /pid %%a >nul 2>&1
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8080 ^| findstr LISTENING') do taskkill /f /pid %%a >nul 2>&1

:: 2. Iniciar Worker Java
echo [2/3] Iniciando Worker Java (Porta 8080)...
if exist "worker-java\run_worker_new.bat" (
    start "Worker Java" cmd /c "cd worker-java && run_worker_new.bat"
) else (
    start "Worker Java" cmd /c "cd worker-java && run_worker.bat"
)

:: 3. Iniciar Portal Next.js
echo [3/3] Iniciando Portal Next.js (Porta 3000)...
start "Portal Portal" cmd /c "start_portal.bat"

echo.
echo ==========================================
echo    SISTEMA EM INICIALIZACAO
echo ==========================================
echo Portal: http://localhost:3000
echo Worker: http://localhost:8080
echo.
echo Pode fechar esta janela, os servicos continuarao rodando.
pause
