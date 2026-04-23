@echo off
set "JAVA_HOME=C:\Program Files\Java\jdk-17"
set "JAVAC=%JAVA_HOME%\bin\javac.exe"

echo [Build] Compilando arquivos Java...

set "CP=src"
set "CP=%CP%;lib\jaybird-4.0.10.java8.jar"
set "CP=%CP%;lib\jackson-databind-2.16.1.jar"
set "CP=%CP%;lib\jackson-core-2.16.1.jar"
set "CP=%CP%;lib\jackson-annotations-2.16.1.jar"
set "CP=%CP%;lib\mysql-connector-j-8.0.33.jar"
set "CP=%CP%;lib\connector-api-1.5.jar"
set "CP=%CP%;lib\commons-compress-1.24.0.jar"
set "CP=%CP%;lib\h2-2.1.214.jar"
set "CP=%CP%;C:\netbeans-28-bin\netbeans\LIBS\flatlaf-3.4.jar"

dir /s /b src\*.java > java_files.txt
"%JAVAC%" -encoding UTF-8 -cp "%CP%" -d src @java_files.txt
if %ERRORLEVEL% NEQ 0 (
    echo [Build] Erro na compilacao.
    del java_files.txt
    exit /b %ERRORLEVEL%
)
del java_files.txt

echo [Build] Concluido. Iniciando Worker...

start "" "C:\Program Files\Java\jdk-17\bin\java.exe" -cp "%CP%;dist\host-migration.jar" br.com.lcsistemas.syspdv.AppWorker
