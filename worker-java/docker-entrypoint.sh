#!/bin/bash
set -e

echo "[Entrypoint] === Iniciando container do Migrador Web ==="

# Prepara diretórios necessários pelo Firebird
mkdir -p /var/run/firebird/3.0
chown -R firebird:firebird /var/run/firebird/3.0 2>/dev/null || true
chmod 777 /var/run/firebird/3.0
mkdir -p /tmp/firebird
chmod 777 /tmp/firebird

# Diagnóstico: quais binários existem
echo "[Entrypoint] --- Binários Firebird ---"
for b in /usr/sbin/firebird /usr/sbin/fbguard /usr/sbin/fb_inet_server \
          /usr/lib/firebird/3.0/bin/fbguard /usr/lib/firebird/3.0/bin/fb_inet_server \
          /usr/bin/gbak /usr/bin/gsec /usr/bin/isql-fb; do
    [ -f "$b" ] && echo "[Entrypoint]   EXISTE: $b ($(ls -la "$b" | awk '{print $1,$3,$4}'))" || true
done

echo "[Entrypoint] --- Security Database ---"
for db in /var/lib/firebird/3.0/system/security3.fdb \
           /var/lib/firebird/3.0/security/security3.fdb \
           /var/lib/firebird/3.0/security3.fdb; do
    [ -f "$db" ] && echo "[Entrypoint]   OK: $db" \
                 || echo "[Entrypoint]   FALTA: $db"
done

echo "[Entrypoint] Iniciando Firebird 3.0..."

# Método 1: init.d (caminho canônico Ubuntu)
echo "[Entrypoint] Tentando /etc/init.d/firebird3.0 start..."
/etc/init.d/firebird3.0 start 2>&1 && echo "[Entrypoint] init.d: OK" \
                                    || echo "[Entrypoint] init.d: erro (continuando)"
sleep 3

if nc -z localhost 3050 2>/dev/null; then
    echo "[Entrypoint] ✅ Firebird ativo após init.d"
else
    echo "[Entrypoint] Porta 3050 ainda fechada — tentando start-stop-daemon direto..."

    # Método 2: start-stop-daemon como init.d faz internamente
    if [ -x /usr/sbin/firebird ]; then
        start-stop-daemon --start --background \
            --chuid firebird:firebird \
            --make-pidfile --pidfile /var/run/firebird/3.0/firebird.pid \
            --exec /usr/sbin/firebird -- -daemon 2>&1 \
          && echo "[Entrypoint] start-stop-daemon: OK" \
          || echo "[Entrypoint] start-stop-daemon: erro (continuando)"
        sleep 4
    fi

    # Método 3: su direto em foreground (fallback mais simples)
    if ! nc -z localhost 3050 2>/dev/null && [ -x /usr/sbin/firebird ]; then
        echo "[Entrypoint] Tentando su firebird foreground..."
        su -s /bin/sh firebird -c '/usr/sbin/firebird 2>&1' &
        sleep 5
    fi

    # Método 4: fbguard se existir
    if ! nc -z localhost 3050 2>/dev/null && [ -x /usr/sbin/fbguard ]; then
        echo "[Entrypoint] Tentando fbguard..."
        su -s /bin/sh firebird -c '/usr/sbin/fbguard 2>&1' &
        sleep 4
    fi
fi

# Aguarda até 45s pela porta 3050
MAX_WAIT=45
count=0
FB_OK=false
while [ "$count" -lt "$MAX_WAIT" ]; do
    if nc -z localhost 3050 2>/dev/null; then
        FB_OK=true
        break
    fi
    count=$((count + 1))
    echo "[Entrypoint] Aguardando porta 3050... ($count/${MAX_WAIT}s)"
    sleep 1
done

if [ "$FB_OK" = "true" ]; then
    echo "[Entrypoint] ✅ Firebird 3.0 pronto na porta 3050"
    # Normaliza SYSDBA=masterkey via SRP (Ubuntu instala Firebird3 com senha SRP vazia)
    if /usr/bin/gsec -user SYSDBA -password "" -modify SYSDBA -pw masterkey 2>/dev/null; then
        echo "[Entrypoint] ✅ SYSDBA: senha vazia → masterkey (SRP)"
    elif /usr/bin/gsec -user SYSDBA -password masterkey -display SYSDBA 2>/dev/null | grep -qi sysdba; then
        echo "[Entrypoint] ✅ SYSDBA já autenticado com masterkey"
    else
        echo "[Entrypoint] ⚠ Não foi possível confirmar senha SYSDBA"
    fi
else
    echo "[Entrypoint] ⚠ Firebird não respondeu em ${MAX_WAIT}s"
    echo "[Entrypoint]   Processos Firebird:"
    ps aux | grep -Ei 'firebird|fbguard|fb_inet' | grep -v grep || echo "  (nenhum)"
    echo "[Entrypoint]   O GerenciadorFirebird Java vai tentar iniciar o binário via /app/FIREBIRD/"
fi

echo "[Entrypoint] Iniciando AppWorker Java na porta ${PORT:-8080}..."
exec java -cp 'app:lib/*' br.com.lcsistemas.syspdv.AppWorker
