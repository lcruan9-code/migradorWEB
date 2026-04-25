FROM eclipse-temurin:17-jdk-jammy AS build

WORKDIR /app

COPY lib ./lib
COPY src/br/com/lcsistemas/syspdv ./src/br/com/lcsistemas/syspdv

RUN set -eux; \
    find src/br/com/lcsistemas/syspdv -name '*.java' > sources.txt; \
    mkdir -p out; \
    javac -encoding UTF-8 -cp "lib/*" -d out @sources.txt; \
    mkdir -p out/br/com/lcsistemas/syspdv/resource; \
    mkdir -p out/br/com/lcsistemas/syspdv/sql; \
    cp src/br/com/lcsistemas/syspdv/resource/banco_novo.sql out/br/com/lcsistemas/syspdv/resource/; \
    cp src/br/com/lcsistemas/syspdv/sql/cidades.csv out/br/com/lcsistemas/syspdv/sql/; \
    cp src/br/com/lcsistemas/syspdv/sql/reference_data.json out/br/com/lcsistemas/syspdv/sql/

# ── Stage: Firebird 2.5.9 embedded tools ─────────────────────────────────────
# jacobalberty/firebird:2.5-sc é uma imagem consolidada do Docker Hub
# com Firebird 2.5 SuperClassic pré-instalado (inclui libfbembed.so).
# Não depende de repositórios apt — evita falhas por pacotes removidos.
FROM jacobalberty/firebird:2.5-sc AS fb25-stage

RUN set -eux; \
    mkdir -p /fb25/lib /fb25/bin; \
    # Descobre o diretório raiz do Firebird instalado (geralmente /usr/local/firebird)
    FB_ROOT=$(find /usr/local /opt -maxdepth 3 -name 'gbak' 2>/dev/null | head -1 \
              | xargs -I{} dirname {} | xargs -I{} dirname {}); \
    echo "=== Firebird root: $FB_ROOT ==="; \
    ls -la "$FB_ROOT/"; \
    # Copia gbak renomeado para não conflitar com gbak do FB 3.0
    cp "$FB_ROOT/bin/gbak" /fb25/bin/gbak25; \
    chmod +x /fb25/bin/gbak25; \
    # Copia todas as libs do diretório Firebird (libfbembed, libfbclient, etc.)
    find "$FB_ROOT/lib" -name '*.so*' | xargs -I{} cp {} /fb25/lib/; \
    # Copia as libs ICU do sistema — jacobalberty não as embute no FB_ROOT/lib
    find /usr/lib /lib -name 'libicuuc.so.*'   2>/dev/null | xargs -I{} cp {} /fb25/lib/ || true; \
    find /usr/lib /lib -name 'libicudata.so.*' 2>/dev/null | xargs -I{} cp {} /fb25/lib/ || true; \
    find /usr/lib /lib -name 'libicui18n.so.*' 2>/dev/null | xargs -I{} cp {} /fb25/lib/ || true; \
    echo "=== ICU libs ===" && ls /fb25/lib/libicu* 2>/dev/null || echo "AVISO: sem ICU"; \
    # Copia o diretório intl/ (libfbintl.so + fbintl.conf) — necessário para character sets
    # (ISO8859_1, UTF-8, etc.). Sem ele: "CHARACTER SET X is not installed"
    if [ -d "$FB_ROOT/intl" ]; then \
        cp -r "$FB_ROOT/intl" /fb25/; \
        echo "=== intl/ ===" && ls -la /fb25/intl/; \
    else \
        echo "AVISO: $FB_ROOT/intl não encontrado — buscando fbintl em outros locais"; \
        find / -name 'fbintl*' 2>/dev/null; \
    fi; \
    # security2.fdb: busca em toda a imagem (pode estar fora de $FB_ROOT)
    echo "=== Buscando security2.fdb ==="; \
    find / -name 'security2.fdb' 2>/dev/null || echo "security2.fdb não encontrado em lugar nenhum"; \
    SEC_DB=$(find / -name 'security2.fdb' 2>/dev/null | head -1); \
    if [ -n "$SEC_DB" ]; then \
        cp "$SEC_DB" /fb25/security2.fdb; \
        chmod 666 /fb25/security2.fdb; \
        echo "security2.fdb copiado de: $SEC_DB"; \
    else \
        echo "AVISO: security2.fdb não encontrado — modo embedded (root) não precisa dele"; \
    fi; \
    # firebird.msg: busca em toda a imagem
    MSG=$(find / -name 'firebird.msg' 2>/dev/null | head -1); \
    [ -n "$MSG" ] && cp "$MSG" /fb25/firebird.msg && echo "firebird.msg: $MSG" || echo "AVISO: firebird.msg não encontrado"; \
    echo "=== /fb25/bin/ ===" && ls -la /fb25/bin/; \
    echo "=== /fb25/lib/ ===" && ls -la /fb25/lib/; \
    echo "=== /fb25/ ===" && ls -la /fb25/

# ── Runtime stage: Ubuntu 22.04 + FB 3.0 + FB 2.5 embedded tools ──────────────────────
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Instala Firebird 3.0 (porta 3050) para todos os sistemas exceto syspdv
# syspdv usa gbak25 embedded (FB 2.5) para converter ODS 11.2 → ODS 12.2 antes de conectar
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        firebird3.0-server \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Configura Firebird 3.0: IPv4, Legacy_Auth, acesso total, cache reduzido (pouca RAM)
RUN sed -i 's/^RemoteBindAddress.*/RemoteBindAddress = 0.0.0.0/' /etc/firebird/3.0/firebird.conf && \
    printf '\nServerMode = SuperServer\nAuthServer = Srp256, Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Disabled\nDatabaseAccess = Full\nDefaultDBCachePages = 256\n' \
    >> /etc/firebird/3.0/firebird.conf

# Diretório de trabalho acessível por Java (root) e Firebird (user firebird)
RUN mkdir -p /app/fdb-work && chmod 777 /app/fdb-work

# Symlinks para GerenciadorFirebird.java
RUN mkdir -p /app/FIREBIRD/Firebird-3.0.manual/bin && \
    ln -sf /usr/bin/gbak /app/FIREBIRD/Firebird-3.0.manual/bin/gbak && \
    { ln -sf /usr/sbin/firebird3.0 /app/FIREBIRD/Firebird-3.0.manual/firebird.exe 2>/dev/null || \
      ln -sf /usr/sbin/firebird    /app/FIREBIRD/Firebird-3.0.manual/firebird.exe 2>/dev/null || true; }

# Copia ferramentas FB 2.5 embedded do stage jacobalberty
COPY --from=fb25-stage /fb25 /opt/fb25

# gbak25 foi compilado com /usr/local/firebird como caminho hardcoded.
# Symlink garante que ele encontre firebird.msg e security2.fdb lá.
RUN ln -sf /opt/fb25 /usr/local/firebird

# Symlink: libfbclient.so.2 → libfbembed.so  (força gbak25 a usar modo embedded)
RUN chmod +x /opt/fb25/bin/gbak25 && \
    FBEMBED=$(ls /opt/fb25/lib/libfbembed.so.* 2>/dev/null | head -1) && \
    if [ -n "$FBEMBED" ]; then \
        ln -sf "$FBEMBED" /opt/fb25/lib/libfbclient.so.2 && \
        ln -sf /opt/fb25/lib/libfbclient.so.2 /opt/fb25/lib/libfbclient.so; \
        echo "[Dockerfile] symlink libfbclient.so.2 → $FBEMBED OK"; \
    else \
        echo "[Dockerfile] AVISO: libfbembed.so não encontrado em /opt/fb25/lib/"; \
        ls -la /opt/fb25/lib/; \
    fi

COPY --from=build /app/out ./app
COPY --from=build /app/lib ./lib

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV PORT=8080
ENV JAVA_TOOL_OPTIONS="-Xmx350m -Dfile.encoding=UTF-8 -Djava.net.preferIPv4Stack=true"

EXPOSE 8080

ENTRYPOINT ["/docker-entrypoint.sh"]
