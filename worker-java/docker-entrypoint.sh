#!/bin/bash
set -e

echo "[Entrypoint] === Iniciando container do Migrador Web ==="

# ── Diretório de PID para Firebird 3.0 ───────────────────────────────────────
mkdir -p /var/run/firebird/3.0
chown -R firebird:firebird /var/run/firebird/3.0 2>/dev/null || true
chmod 777 /var/run/firebird/3.0
mkdir -p /tmp/firebird && chmod 777 /tmp/firebird

# ── Firebird 3.0 na porta 3050 ────────────────────────────────────────────────
echo "[Entrypoint] Iniciando Firebird 3.0 (porta 3050)..."
/etc/init.d/firebird3.0 start 2>&1 && echo "[Entrypoint] FB3.0 init.d: OK" \
                                    || echo "[Entrypoint] FB3.0 init.d: erro (continuando)"
sleep 3

if ! nc -z localhost 3050 2>/dev/null; then
    echo "[Entrypoint] Tentando start-stop-daemon para FB3.0..."
    if [ -x /usr/sbin/firebird3.0 ]; then
        start-stop-daemon --start --background \
            --chuid firebird:firebird \
            --make-pidfile --pidfile /var/run/firebird/3.0/firebird.pid \
            --exec /usr/sbin/firebird3.0 -- -daemon 2>&1 \
          && echo "[Entrypoint] FB3.0 start-stop-daemon: OK" \
          || echo "[Entrypoint] FB3.0 start-stop-daemon: erro"
        sleep 4
    fi
    if ! nc -z localhost 3050 2>/dev/null && [ -x /usr/sbin/firebird ]; then
        su -s /bin/sh firebird -c '/usr/sbin/firebird 2>&1' &
        sleep 4
    fi
fi

# Aguarda porta 3050
count=0; FB30_OK=false
while [ "$count" -lt 30 ]; do
    nc -z localhost 3050 2>/dev/null && FB30_OK=true && break
    count=$((count+1)); sleep 1
done

if [ "$FB30_OK" = "true" ]; then
    echo "[Entrypoint] ✅ Firebird 3.0 pronto na porta 3050"
    # Normaliza SYSDBA
    /usr/bin/gsec -user SYSDBA -password "" -modify SYSDBA -pw masterkey 2>/dev/null && \
        echo "[Entrypoint] ✅ FB3.0 SYSDBA: senha vazia → masterkey" || \
        echo "[Entrypoint]    FB3.0 SYSDBA: já com masterkey (ou não precisou)"
else
    echo "[Entrypoint] ⚠ Firebird 3.0 não respondeu na porta 3050"
fi

# ── Verifica gbak25 (FB 2.5 embedded — para conversão ODS 11.2 → 12.2) ───────
if [ -x /opt/fb25/bin/gbak25 ]; then
    echo "[Entrypoint] ✅ gbak25 (FB 2.5 embedded) disponível em /opt/fb25/bin/gbak25"
    ls -la /opt/fb25/lib/libfbembed.so.* 2>/dev/null || echo "[Entrypoint] ⚠ libfbembed não encontrado"
    ls -la /opt/fb25/security2.fdb 2>/dev/null       || echo "[Entrypoint] ⚠ security2.fdb não encontrado em /opt/fb25/"
else
    echo "[Entrypoint] ⚠ gbak25 não encontrado — conversão ODS 11.2 não disponível"
fi

echo "[Entrypoint] Iniciando AppWorker Java na porta ${PORT:-8080}..."
exec java -cp 'app:lib/*' br.com.lcsistemas.syspdv.AppWorker
