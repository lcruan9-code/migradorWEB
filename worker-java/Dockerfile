FROM eclipse-temurin:17-jdk-jammy AS build

WORKDIR /app

COPY lib ./lib
COPY src ./src

RUN set -eux; \
    find src -name '*.java' > sources.txt; \
    mkdir -p out; \
    javac -encoding UTF-8 -cp "lib/*" -d out @sources.txt; \
    cp -R src/. out/

FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

COPY --from=build /app/out ./app
COPY --from=build /app/lib ./lib

ENV PORT=8080
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -Dfile.encoding=UTF-8"

EXPOSE 8080

CMD ["sh", "-lc", "java -cp 'app:lib/*' br.com.lcsistemas.syspdv.AppWorker"]
