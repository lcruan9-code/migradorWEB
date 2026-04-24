#!/bin/bash
set -e

echo "[Entrypoint] === Iniciando container do Migrador Web ==="

# Garante que o diretório pid do Firebird exista com permissões corretas
mkdir -p /var/run/firebird/3.0
chown -R firebird:firebird /var/run/firebird/3.0 2>/dev/null || true
chmod 777 /var/run/firebird/3.0

echo "[Entrypoint] Iniciando Firebird 3.0..."

# Tenta via init.d primeiro (Ubuntu padrão)
if /etc/init.d/firebird3.0 start 2>&1; then
    echo "[Entrypoint] Firebird iniciado via init.d"
else
    echo "[Entrypoint] init.d falhou — tentando binário direto..."
    # Fallback: inicia o processo SuperClassic diretamente como user firebird
    if command -v fb_inet_server &>/dev/null; then
        su -s /bin/sh firebird -c 'fb_inet_server &' 2>/dev/null || \
        fb_inet_server &
        echo "[Entrypoint] fb_inet_server iniciado"
    elif [ -f /usr/sbin/firebird ]; then
        su -s /bin/sh firebird -c '/usr/sbin/firebird &' 2>/dev/null || \
        /usr/sbin/firebird &
        echo "[Entrypoint] /usr/sbin/firebird iniciado"
    else
        echo "[Entrypoint] AVISO: nenhum binário Firebird encontrado — migração usará fallback"
    fi
fi

# Aguarda Firebird estar pronto na porta 3050 (max 30s)
MAX_WAIT=30
count=0
FB_OK=false
while [ "$count" -lt "$MAX_WAIT" ]; do
    if nc -z localhost 3050 2>/dev/null; then
        FB_OK=true
        break
    fi
    count=$((count + 1))
    echo "[Entrypoint] Aguardando Firebird na porta 3050... ($count/${MAX_WAIT}s)"
    sleep 1
done

if [ "$FB_OK" = "true" ]; then
    echo "[Entrypoint] ✅ Firebird 3.0 pronto na porta 3050"
    # Garante senha SYSDBA=masterkey (idempotente, falha silenciosa se já ok)
    echo "modify SYSDBA -pw masterkey" | \
        /usr/bin/gsec -user SYSDBA -password masterkey 2>/dev/null || \
    echo "modify SYSDBA -pw masterkey" | \
        /usr/bin/gsec -user SYSDBA -password "" 2>/dev/null || true
else
    echo "[Entrypoint] ⚠ Firebird nao respondeu em ${MAX_WAIT}s"
    echo "[Entrypoint]   A migração de .FDB vai falhar — verifique os logs acima"
fi

echo "[Entrypoint] Iniciando AppWorker Java na porta ${PORT:-8080}..."
exec java -cp 'app:lib/*' br.com.lcsistemas.syspdv.AppWorker
