@echo off
SETLOCAL EnableDelayedExpansion
TITLE Migrador Web - Stop All Systems
COLOR 0C

echo ==========================================
echo    PARANDO TODO O SISTEMA MIGRACAO
echo ==========================================
echo.

set "found=0"

echo Procurando Portal (Porta 3000)...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :3000 ^| findstr LISTENING') do (
    echo Parando Portal PID: %%a
    taskkill /f /pid %%a >nul 2>&1
    set "found=1"
)

echo Procurando Worker (Porta 8080)...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8080 ^| findstr LISTENING') do (
    echo Parando Worker PID: %%a
    taskkill /f /pid %%a >nul 2>&1
    set "found=1"
)

if "!found!"=="1" (
    echo.
    echo [SUCESSO] Sistema parado com sucesso.
) else (
    echo.
    echo [AVISO] Nenhum servico ativo encontrado.
)

echo.
pause
