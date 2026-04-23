$JavaHome = "C:\Program Files\Java\jdk-17"
$Javac = "$JavaHome\bin\javac.exe"
$Java = "$JavaHome\bin\java.exe"

Write-Host "[Build] Limpando processos antigos..."
taskkill /F /IM java.exe 2>$null

# Encontra todos os JARs e monta o classpath entre aspas
$Libs = Get-ChildItem "lib\*.jar" | ForEach-Object { "$($_.FullName)" }
$Classpath = "src;" + ($Libs -join ";")

# Encontra todos os arquivos .java do syspdv
$JavaFiles = Get-ChildItem "src\br\com\lcsistemas\syspdv\*.java" -Recurse | ForEach-Object { "$($_.FullName)" }

Write-Host "[Build] Compilando arquivos Java (UTF-8)..."
# Adicionado -encoding utf8 para evitar erros com caracteres especiais nos comentarios
& $Javac -encoding utf8 -cp "$Classpath" -d src $JavaFiles

if ($LASTEXITCODE -eq 0) {
    Write-Host "[Build] Concluido. Iniciando Worker..."
    Start-Process -FilePath $Java -ArgumentList "-cp", "`"$Classpath;dist\host-migration.jar`"", "br.com.lcsistemas.syspdv.AppWorker"
} else {
    Write-Host "[Build] Erro na compilacao. Tentando compilar apenas pacotes criticos..."
    
    # Fallback: tenta compilar apenas os pacotes que mexemos (sql e step)
    $TargetFiles = Get-ChildItem "src\br\com\lcsistemas\syspdv\sql\*.java", "src\br\com\lcsistemas\syspdv\step\*.java", "src\br\com\lcsistemas\syspdv\core\*.java" | ForEach-Object { $_.FullName }
    & $Javac -encoding utf8 -cp "$Classpath" -d src $TargetFiles
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[Build] Compilacao parcial concluida. Iniciando Worker..."
        Start-Process -FilePath $Java -ArgumentList "-cp", "`"$Classpath;dist\host-migration.jar`"", "br.com.lcsistemas.syspdv.AppWorker"
    } else {
        Write-Host "[Build] Falha critica na compilacao."
    }
}
