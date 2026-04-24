#!/bin/bash
set -e

echo "[Entrypoint] Iniciando Firebird 3.0..."
service firebird3.0 start || true

# Aguarda Firebird estar pronto na porta 3050 (max 30s)
MAX_WAIT=30
count=0
while ! nc -z localhost 3050 2>/dev/null; do
    count=$((count + 1))
    if [ "$count" -ge "$MAX_WAIT" ]; then
        echo "[Entrypoint] AVISO: Firebird nao respondeu em ${MAX_WAIT}s — continuando assim mesmo"
        break
    fi
    echo "[Entrypoint] Aguardando Firebird... ($count/${MAX_WAIT}s)"
    sleep 1
done

if nc -z localhost 3050 2>/dev/null; then
    echo "[Entrypoint] Firebird 3.0 pronto na porta 3050"

    # Garante senha do SYSDBA = masterkey (idempotente)
    echo "modify SYSDBA -pw masterkey" | \
        /usr/bin/gsec -user SYSDBA -password masterkey 2>/dev/null || \
    echo "modify SYSDBA -pw masterkey" | \
        /usr/bin/gsec -user SYSDBA -password "" 2>/dev/null || true
fi

echo "[Entrypoint] Iniciando AppWorker Java..."
exec java -cp 'app:lib/*' br.com.lcsistemas.syspdv.AppWorker
