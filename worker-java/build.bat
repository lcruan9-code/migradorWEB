@echo off
SETLOCAL
TITLE Compilando Worker Java
COLOR 0A

echo ==========================================
echo    COMPILANDO WORKER JAVA
echo ==========================================
echo.

:: Define paths
set "JAVA_HOME=C:\Program Files\Java\jdk-17"
set "BIN=%JAVA_HOME%\bin"
set "LIB=lib"
set "SRC=src"
set "OUT=bin"

:: Create output dir if not exists
if not exist "%OUT%" mkdir "%OUT%"

:: Build classpath
set "CP=%SRC%;%LIB%\jaybird-4.0.10.java8.jar;%LIB%\jackson-databind-2.16.1.jar;%LIB%\jackson-core-2.16.1.jar;%LIB%\jackson-annotations-2.16.1.jar;%LIB%\mysql-connector-j-8.0.33.jar;%LIB%\connector-api-1.5.jar;%LIB%\commons-compress-1.24.0.jar;%LIB%\h2-2.1.214.jar;C:\netbeans-28-bin\netbeans\LIBS\flatlaf-3.4.jar"

echo [1/2] Compilando arquivos .java...
"%BIN%\javac.exe" -encoding UTF-8 -d "%OUT%" -cp "%CP%" src\br\com\lcsistemas\syspdv\AppWorker.java src\br\com\lcsistemas\syspdv\engine\MigracaoEngine.java src\br\com\lcsistemas\syspdv\sql\SqlFileWriter.java src\br\com\lcsistemas\syspdv\sql\SqlFileRunner.java

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERRO] Falha na compilacao! Verifique os erros acima.
    pause
    exit /b %ERRORLEVEL%
)

echo [2/2] Copiando recursos...
if not exist "%OUT%\br\com\lcsistemas\syspdv\sql" mkdir "%OUT%\br\com\lcsistemas\syspdv\sql"
if not exist "%OUT%\br\com\lcsistemas\syspdv\resource" mkdir "%OUT%\br\com\lcsistemas\syspdv\resource"
if exist "%SRC%\br\com\lcsistemas\syspdv\resource" (
    xcopy /s /e /y /i "%SRC%\br\com\lcsistemas\syspdv\resource\*" "%OUT%\br\com\lcsistemas\syspdv\resource\" >nul
)
if exist "%SRC%\br\com\lcsistemas\syspdv\resource\banco_novo.sql" (
    copy /y "%SRC%\br\com\lcsistemas\syspdv\resource\banco_novo.sql" "%OUT%\br\com\lcsistemas\syspdv\resource\" >nul
)
robocopy "%SRC%\br\com\lcsistemas\syspdv" "%OUT%\br\com\lcsistemas\syspdv" *.class /S /NFL /NDL /NJH /NJS /NC /NS ^
    /XF AppWorker.class AppWorker$*.class MigracaoEngine.class MigracaoEngine$*.class SqlFileWriter.class SqlFileWriter$*.class SqlFileRunner.class SqlFileRunner$*.class >nul
copy /y "%SRC%\br\com\lcsistemas\syspdv\sql\cidades.csv" "%OUT%\br\com\lcsistemas\syspdv\sql\" >nul
if exist "%SRC%\br\com\lcsistemas\syspdv\sql\reference_data.json" (
    copy /y "%SRC%\br\com\lcsistemas\syspdv\sql\reference_data.json" "%OUT%\br\com\lcsistemas\syspdv\sql\" >nul
)
for %%f in ("%SRC%\br\com\lcsistemas\syspdv\sql\*.class") do (
    if /I not "%%~nxf"=="SqlFileRunner.class" if /I not "%%~nxf"=="SqlFileRunner$LogCallback.class" if /I not "%%~nxf"=="SqlFileWriter.class" if /I not "%%~nxf"=="SqlFileWriter$SqlLiteral.class" copy /y "%%f" "%OUT%\br\com\lcsistemas\syspdv\sql\" >nul
)

echo.
echo ==========================================
echo    COMPILACAO CONCLUIDA COM SUCESSO!
echo ==========================================
echo.
echo Agora voce pode usar o run_worker_new.bat para rodar a versao compilada.
echo.

:: Create a new run script that uses the bin folder
(
echo @echo off
echo cd /d "%%~dp0"
echo "C:\Program Files\Java\jdk-17\bin\java.exe" -cp "bin;lib\jaybird-4.0.10.java8.jar;lib\jackson-databind-2.16.1.jar;lib\jackson-core-2.16.1.jar;lib\jackson-annotations-2.16.1.jar;lib\mysql-connector-j-8.0.33.jar;lib\connector-api-1.5.jar;lib\commons-compress-1.24.0.jar;lib\h2-2.1.214.jar;C:\netbeans-28-bin\netbeans\LIBS\flatlaf-3.4.jar" br.com.lcsistemas.syspdv.AppWorker
) > run_worker_new.bat

pause
