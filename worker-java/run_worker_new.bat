@echo off
cd /d "%~dp0"
"C:\Program Files\Java\jdk-17\bin\java.exe" -cp "bin;lib\jaybird-4.0.10.java8.jar;lib\jackson-databind-2.16.1.jar;lib\jackson-core-2.16.1.jar;lib\jackson-annotations-2.16.1.jar;lib\mysql-connector-j-8.0.33.jar;lib\connector-api-1.5.jar;lib\commons-compress-1.24.0.jar;lib\h2-2.1.214.jar;C:\netbeans-28-bin\netbeans\LIBS\flatlaf-3.4.jar" br.com.lcsistemas.syspdv.AppWorker
