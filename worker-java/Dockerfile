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

# ── Stage: Firebird 2.5 embedded tools (Ubuntu 18.04 Bionic — único com FB 2.5 no apt) ──
FROM ubuntu:bionic AS fb25-stage

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        firebird2.5-superserver \
        firebird2.5-utils \
    && rm -rf /var/lib/apt/lists/*

# Coleta tudo que o gbak25 precisa para rodar em modo embedded no Ubuntu 22.04
RUN mkdir -p /fb25/lib /fb25/bin && \
    # gbak binary (renomeado para não conflitar com o gbak do FB 3.0)
    cp /usr/bin/gbak /fb25/bin/gbak25 && \
    # libfbembed: engine embedded do FB 2.5 (lê ODS 11.2 diretamente, sem server)
    find /usr/lib -name 'libfbembed.so.2.5*'  | head -1 | xargs -I{} cp {} /fb25/lib/ && \
    find /usr/lib -name 'libfbclient.so.2.5*' | head -1 | xargs -I{} cp {} /fb25/lib/ && \
    # ICU 60 — FB 2.5 foi compilado contra essa versão; Ubuntu 22.04 tem ICU 70 (ABI incompatível)
    cp /usr/lib/x86_64-linux-gnu/libicuuc.so.60*    /fb25/lib/ && \
    cp /usr/lib/x86_64-linux-gnu/libicudata.so.60*  /fb25/lib/ && \
    cp /usr/lib/x86_64-linux-gnu/libicui18n.so.60*  /fb25/lib/ 2>/dev/null || true && \
    # Security DB em /fb25/ (raiz) — libfbembed busca security2.fdb via $FIREBIRD env var
    find /var/lib/firebird /usr/lib/firebird -name 'security2.fdb' 2>/dev/null | head -1 \
        | xargs -I{} cp {} /fb25/ && \
    find /usr -name 'firebird.msg' 2>/dev/null | head -1 | xargs -I{} cp {} /fb25/ && \
    chmod 644 /fb25/security2.fdb 2>/dev/null || true && \
    echo "=== FB25 stage contents ===" && ls -la /fb25/ && ls -la /fb25/lib/ && ls -la /fb25/bin/

# ── Runtime stage: Ubuntu 22.04 + FB 3.0 + FB 2.5 embedded tools ──────────────────────
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Instala Firebird 3.0 (porta 3050) para demais sistemas (ODS 12+)
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        firebird3.0-server \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Configura Firebird 3.0: IPv4, Legacy_Auth, acesso total
RUN sed -i 's/^RemoteBindAddress.*/RemoteBindAddress = 0.0.0.0/' /etc/firebird/3.0/firebird.conf && \
    printf '\nServerMode = SuperServer\nAuthServer = Srp256, Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Disabled\nDatabaseAccess = Full\n' \
    >> /etc/firebird/3.0/firebird.conf

# Diretório de trabalho acessível por Java (root) e Firebird (user firebird)
RUN mkdir -p /app/fdb-work && chmod 777 /app/fdb-work

# Symlinks para GerenciadorFirebird.java
RUN mkdir -p /app/FIREBIRD/Firebird-3.0.manual/bin && \
    ln -sf /usr/bin/gbak /app/FIREBIRD/Firebird-3.0.manual/bin/gbak && \
    { ln -sf /usr/sbin/firebird3.0 /app/FIREBIRD/Firebird-3.0.manual/firebird.exe 2>/dev/null || \
      ln -sf /usr/sbin/firebird    /app/FIREBIRD/Firebird-3.0.manual/firebird.exe 2>/dev/null || true; }

# Copia ferramentas FB 2.5 embedded do stage bionic
COPY --from=fb25-stage /fb25 /opt/fb25

# Symlinks de compatibilidade: quando gbak25 buscar libfbclient.so.2, encontra a embedded
RUN chmod +x /opt/fb25/bin/gbak25 && \
    ln -sf /opt/fb25/lib/libfbembed.so.2.5.9  /opt/fb25/lib/libfbclient.so.2 2>/dev/null || \
    ln -sf "$(ls /opt/fb25/lib/libfbembed.so.* | head -1)" /opt/fb25/lib/libfbclient.so.2 && \
    ln -sf /opt/fb25/lib/libfbclient.so.2 /opt/fb25/lib/libfbclient.so && \
    ldconfig 2>/dev/null || true

COPY --from=build /app/out ./app
COPY --from=build /app/lib ./lib

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV PORT=8080
ENV JAVA_TOOL_OPTIONS="-Xmx200m -Dfile.encoding=UTF-8 -Djava.net.preferIPv4Stack=true"

EXPOSE 8080

ENTRYPOINT ["/docker-entrypoint.sh"]
