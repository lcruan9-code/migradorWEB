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

FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Instala Firebird 3.0 (servidor + ferramentas) e netcat para health check
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        firebird3.0-server \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Configura Firebird 3.0 para aceitar autenticação legacy (compatível com Jaybird 4)
# e desabilita criptografia do wire para simplificar a conexão local
RUN printf '\nAuthServer = Legacy_Auth, Srp256, Srp\nUserManager = Legacy_UserManager, Srp\nWireCrypt = Disabled\nDatabaseAccess = Full\n' \
    >> /etc/firebird/3.0/firebird.conf

# Diretório de trabalho acessível tanto pelo Java (root) quanto pelo Firebird (user firebird)
RUN mkdir -p /app/fdb-work && chmod 777 /app/fdb-work

# Cria o caminho esperado pelo resolverGbak() do Java apontando para o gbak do sistema
RUN mkdir -p /app/FIREBIRD/Firebird-3.0.manual/bin && \
    ln -sf /usr/bin/gbak /app/FIREBIRD/Firebird-3.0.manual/bin/gbak

COPY --from=build /app/out ./app
COPY --from=build /app/lib ./lib

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV PORT=8080
ENV JAVA_TOOL_OPTIONS="-Xmx200m -Dfile.encoding=UTF-8"

EXPOSE 8080

ENTRYPOINT ["/docker-entrypoint.sh"]
