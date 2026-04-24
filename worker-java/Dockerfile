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

# Instala Firebird 3.0 server + ferramentas (único pacote disponível no Ubuntu 22.04 Jammy)
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        firebird3.0-server \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Configura Firebird 3.0:
#   ServerMode = SuperServer → processo persistente ouvindo na porta 3050 (sem inetd)
#   AuthServer/UserManager → autenticação legacy para Jaybird 4
#   WireCrypt Disabled → simplifica conexão local
#   DatabaseAccess Full → acessa qualquer path de .fdb
RUN printf '\nServerMode = SuperServer\nAuthServer = Legacy_Auth, Srp256, Srp\nUserManager = Legacy_UserManager, Srp\nWireCrypt = Disabled\nDatabaseAccess = Full\n' \
    >> /etc/firebird/3.0/firebird.conf

# Diretório de trabalho acessível tanto pelo Java (root) quanto pelo Firebird (user firebird)
RUN mkdir -p /app/fdb-work && chmod 777 /app/fdb-work

# Cria a estrutura esperada pelo GerenciadorFirebird.java:
#   tentarMapear() procura "firebird.exe" na raiz da pasta de instalação
#   iniciarInstalacao() executa: firebird.exe -a
# No Linux, "firebird.exe" é um nome de arquivo válido — o symlink aponta para o binário real.
RUN mkdir -p /app/FIREBIRD/Firebird-3.0.manual/bin && \
    ln -sf /usr/bin/gbak /app/FIREBIRD/Firebird-3.0.manual/bin/gbak && \
    ln -sf /usr/sbin/firebird /app/FIREBIRD/Firebird-3.0.manual/firebird.exe

COPY --from=build /app/out ./app
COPY --from=build /app/lib ./lib

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV PORT=8080
ENV JAVA_TOOL_OPTIONS="-Xmx200m -Dfile.encoding=UTF-8"

EXPOSE 8080

ENTRYPOINT ["/docker-entrypoint.sh"]
