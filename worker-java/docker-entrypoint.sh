#!/bin/bash
set -e

echo "[Entrypoint] === Iniciando container do Migrador Web ==="

# Garante que o diretório pid do Firebird exista com permissões corretas
mkdir -p /var/run/firebird/3.0
chown -R firebird:firebird /var/run/firebird/3.0 2>/dev/null || true
chmod 777 /var/run/firebird/3.0

echo "[Entrypoint] Iniciando Firebird 3.0..."

# fbguard é o processo correto para iniciar o Firebird em todos os modos.
# No SuperServer mode (firebird.conf), ele cria um daemon que escuta na porta 3050.
FB_STARTED=false

if [ -x /usr/sbin/fbguard ]; then
    echo "[Entrypoint] Iniciando fbguard como user firebird..."
    # Roda fbguard como user 'firebird'; sem -f ele se daemoniza automaticamente
    su -s /bin/sh firebird -c '/usr/sbin/fbguard' 2>&1 &
    sleep 1
    FB_STARTED=true
    echo "[Entrypoint] fbguard acionado"
fi

# Fallback 1: init.d (pode não funcionar sem systemd, mas tenta)
if [ "$FB_STARTED" = "false" ]; then
    echo "[Entrypoint] fbguard não encontrado — tentando init.d..."
    if /etc/init.d/firebird3.0 start 2>&1; then
        echo "[Entrypoint] Firebird iniciado via init.d"
        FB_STARTED=true
    fi
fi

# Fallback 2: binário de superserver direto
if [ "$FB_STARTED" = "false" ]; then
    echo "[Entrypoint] Tentando /usr/sbin/firebird (superserver)..."
    if [ -x /usr/sbin/firebird ]; then
        su -s /bin/sh firebird -c '/usr/sbin/firebird' 2>&1 &
        FB_STARTED=true
    fi
fi

if [ "$FB_STARTED" = "false" ]; then
    echo "[Entrypoint] AVISO: nenhum binário Firebird encontrado — migração usará fallback"
fi

# Aguarda Firebird estar pronto na porta 3050 (max 40s)
MAX_WAIT=40
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
    # Garante senha SYSDBA=masterkey (idempotente)
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
