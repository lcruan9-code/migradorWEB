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

# Ubuntu 20.04 (Focal) — tem Firebird 2.5 E 3.0 nos repositórios oficiais
FROM eclipse-temurin:17-jre-focal

WORKDIR /app

# Firebird 2.5 (porta 3051) → syspdv / ODS 11.2
# Firebird 3.0 (porta 3050) → demais sistemas / ODS 12+
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        firebird2.5-superserver \
        firebird2.5-utils \
        firebird3.0-server \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Configura Firebird 3.0 na porta 3050 (IPv4, Legacy_Auth, acesso total)
RUN sed -i 's/^RemoteBindAddress.*/RemoteBindAddress = 0.0.0.0/' /etc/firebird/3.0/firebird.conf && \
    printf '\nServerMode = SuperServer\nAuthServer = Srp256, Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Disabled\nDatabaseAccess = Full\n' \
    >> /etc/firebird/3.0/firebird.conf

# Configura Firebird 2.5 na porta 3051 (separada da 3050, sem conflito)
RUN sed -i '/^RemoteServicePort/d; /^RemoteBindAddress/d; /^DatabaseAccess/d' \
        /etc/firebird/2.5/firebird.conf 2>/dev/null || true && \
    printf '\nRemoteServicePort = 3051\nRemoteBindAddress = 0.0.0.0\nDatabaseAccess = Full\n' \
    >> /etc/firebird/2.5/firebird.conf

# Diretório de trabalho acessível por Java (root) e Firebird (user firebird)
RUN mkdir -p /app/fdb-work && chmod 777 /app/fdb-work

# Symlinks para GerenciadorFirebird.java (procura firebird.exe na pasta de instalação)
RUN mkdir -p /app/FIREBIRD/Firebird-3.0.manual/bin && \
    ln -sf /usr/bin/gbak      /app/FIREBIRD/Firebird-3.0.manual/bin/gbak && \
    ln -sf /usr/sbin/firebird3.0 /app/FIREBIRD/Firebird-3.0.manual/firebird.exe 2>/dev/null || \
    ln -sf /usr/sbin/firebird    /app/FIREBIRD/Firebird-3.0.manual/firebird.exe 2>/dev/null || true

COPY --from=build /app/out ./app
COPY --from=build /app/lib ./lib

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV PORT=8080
ENV JAVA_TOOL_OPTIONS="-Xmx200m -Dfile.encoding=UTF-8 -Djava.net.preferIPv4Stack=true"

EXPOSE 8080

ENTRYPOINT ["/docker-entrypoint.sh"]
