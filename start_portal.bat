@echo off
SETLOCAL
TITLE Migrador Web - Portal Starter
COLOR 0B

echo ==========================================
echo    INICIANDO PORTAL MIGRADOR WEB
echo ==========================================
echo.
echo [1/2] Navegando para a pasta portal...
cd /d "%~dp0portal"

echo [2/2] Executando npm run dev...
echo O portal estara disponivel em: http://localhost:3000
echo.
npm run dev

pause
