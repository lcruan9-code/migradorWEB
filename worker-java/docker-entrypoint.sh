#!/bin/bash
set -e

echo "[Entrypoint] === Iniciando container do Migrador Web ==="

# ── Diretórios de PID para ambas as versões do Firebird ──────────────────────
for dir in /var/run/firebird/3.0 /var/run/firebird/2.5; do
    mkdir -p "$dir"
    chown -R firebird:firebird "$dir" 2>/dev/null || true
    chmod 777 "$dir"
done
mkdir -p /tmp/firebird && chmod 777 /tmp/firebird

# ── Firebird 3.0 na porta 3050 (para GDoor, Host, Clipp, etc.) ───────────────
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

# ── Firebird 2.5 na porta 3051 (para syspdv / ODS 11.2) ─────────────────────
echo "[Entrypoint] Iniciando Firebird 2.5 (porta 3051)..."
/etc/init.d/firebird2.5-superserver start 2>&1 && echo "[Entrypoint] FB2.5 init.d: OK" \
                                               || echo "[Entrypoint] FB2.5 init.d: erro (continuando)"
sleep 3

if ! nc -z localhost 3051 2>/dev/null; then
    echo "[Entrypoint] Tentando start-stop-daemon para FB2.5..."
    for bin in /usr/sbin/firebird2.5-superserver /usr/lib/firebird/2.5/bin/fbguard; do
        [ -x "$bin" ] || continue
        start-stop-daemon --start --background \
            --chuid firebird:firebird \
            --make-pidfile --pidfile /var/run/firebird/2.5/firebird.pid \
            --exec "$bin" 2>&1 \
          && echo "[Entrypoint] FB2.5 start-stop-daemon ($bin): OK" \
          || echo "[Entrypoint] FB2.5 start-stop-daemon ($bin): erro"
        sleep 4
        nc -z localhost 3051 2>/dev/null && break
    done
fi

# Aguarda porta 3051
count=0; FB25_OK=false
while [ "$count" -lt 30 ]; do
    nc -z localhost 3051 2>/dev/null && FB25_OK=true && break
    count=$((count+1)); sleep 1
done

if [ "$FB25_OK" = "true" ]; then
    echo "[Entrypoint] ✅ Firebird 2.5 pronto na porta 3051"
    # Normaliza SYSDBA para FB2.5 (security2.fdb — legacy auth)
    for gsec_bin in /usr/bin/gsec /usr/lib/firebird/2.5/bin/gsec; do
        [ -x "$gsec_bin" ] || continue
        if $gsec_bin -host localhost -port 3051 -user SYSDBA -password "" \
                -modify SYSDBA -pw masterkey 2>/dev/null; then
            echo "[Entrypoint] ✅ FB2.5 SYSDBA: senha vazia → masterkey"
        else
            echo "[Entrypoint]    FB2.5 SYSDBA: tentando confirmar com masterkey..."
            $gsec_bin -host localhost -port 3051 -user SYSDBA -password masterkey \
                -display SYSDBA 2>/dev/null | grep -qi sysdba && \
                echo "[Entrypoint] ✅ FB2.5 SYSDBA já com masterkey" || \
                echo "[Entrypoint] ⚠ FB2.5 SYSDBA: não confirmado"
        fi
        break
    done
else
    echo "[Entrypoint] ⚠ Firebird 2.5 não respondeu na porta 3051 — syspdv não funcionará"
fi

echo "[Entrypoint] Iniciando AppWorker Java na porta ${PORT:-8080}..."
exec java -cp 'app:lib/*' br.com.lcsistemas.syspdv.AppWorker
