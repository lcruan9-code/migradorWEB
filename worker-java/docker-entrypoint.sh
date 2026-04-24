#!/bin/bash
set -e

echo "[Entrypoint] === Iniciando container do Migrador Web ==="

# Garante que o diretório pid do Firebird exista com permissões corretas
mkdir -p /var/run/firebird/3.0
chown -R firebird:firebird /var/run/firebird/3.0 2>/dev/null || true
chmod 777 /var/run/firebird/3.0

# Diagnóstico: lista binários Firebird disponíveis
echo "[Entrypoint] Binários Firebird encontrados:"
for b in /usr/sbin/firebird /usr/sbin/fbguard /usr/sbin/fb_inet_server \
          /usr/lib/firebird/3.0/bin/fbguard /usr/lib/firebird/3.0/bin/fb_inet_server; do
    [ -f "$b" ] && echo "  $b ($(ls -la $b | awk '{print $1,$3,$4}'))" || true
done

echo "[Entrypoint] Iniciando Firebird 3.0..."

# Método 1: init.d (usa start-stop-daemon corretamente para o pacote Ubuntu)
echo "[Entrypoint] Tentando /etc/init.d/firebird3.0 start..."
/etc/init.d/firebird3.0 start 2>&1 || true

# Aguarda brevemente para ver se o init.d funcionou
sleep 2
if nc -z localhost 3050 2>/dev/null; then
    echo "[Entrypoint] ✅ Firebird respondeu após init.d"
else
    echo "[Entrypoint] init.d não abriu a porta — tentando startup direto..."

    # Método 2: /usr/sbin/firebird diretamente (guardian do pacote Ubuntu 22.04)
    if [ -x /usr/sbin/firebird ]; then
        echo "[Entrypoint] Iniciando /usr/sbin/firebird como user firebird..."
        su -s /bin/sh firebird -c '/usr/sbin/firebird -daemon -pidfile /var/run/firebird/3.0/firebird.pid' 2>&1 &
        sleep 2
    fi

    # Método 3: fbguard (Ubuntu 18.04 e anteriores)
    if ! nc -z localhost 3050 2>/dev/null && [ -x /usr/sbin/fbguard ]; then
        echo "[Entrypoint] Tentando /usr/sbin/fbguard..."
        su -s /bin/sh firebird -c '/usr/sbin/fbguard' 2>&1 &
        sleep 2
    fi

    # Método 4: fb_inet_server em modo standalone
    if ! nc -z localhost 3050 2>/dev/null && [ -x /usr/sbin/fb_inet_server ]; then
        echo "[Entrypoint] Tentando fb_inet_server standalone..."
        su -s /bin/sh firebird -c '/usr/sbin/fb_inet_server' 2>&1 &
        sleep 2
    fi
fi

# Aguarda Firebird estar pronto na porta 3050 (max 45s)
MAX_WAIT=45
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
    echo "[Entrypoint]   Processos ativos:"
    ps aux | grep -i firebird | grep -v grep || true
    echo "[Entrypoint]   A migração de .FDB vai falhar — verifique os logs acima"
fi

echo "[Entrypoint] Iniciando AppWorker Java na porta ${PORT:-8080}..."
exec java -cp 'app:lib/*' br.com.lcsistemas.syspdv.AppWorker
