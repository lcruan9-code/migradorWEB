@echo off
SETLOCAL EnableDelayedExpansion
TITLE Migrador Web - Stop Portal
COLOR 0C

echo ==========================================
echo    PARANDO PORTAL MIGRADOR WEB
echo ==========================================
echo.
echo Procurando processo na porta 3000...

set "found=0"
for /f "tokens=5" %%a in ('netstat -aon ^| findstr LISTENING ^| findstr :3000') do (
    set "pid=%%a"
    if not "!pid!"=="0" (
        echo Parando processo PID: !pid!
        taskkill /f /pid !pid! >nul 2>&1
        set "found=1"
    )
)

if "!found!"=="1" (
    echo.
    echo [SUCESSO] Portal parado com sucesso.
) else (
    echo.
    echo [AVISO] Nenhum processo em LISTENING encontrado na porta 3000.
)

echo.
echo Pressione qualquer tecla para sair...
pause >nul
