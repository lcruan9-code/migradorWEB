@echo off
chcp 65001 >nul
title OCI Retry — VM.Standard.A1.Flex

echo.
echo  ==========================================
echo   OCI Retry — Criacao da VM ARM (24 GB)
echo   Tentando a cada 60 segundos...
echo   Feche esta janela para parar.
echo  ==========================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0retry-vm.ps1"

echo.
pause
