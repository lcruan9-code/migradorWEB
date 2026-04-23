# =============================================================================
#  setup-libs.ps1 — Copia as dependências do projeto antigo para este projeto
#  Execute UMA VEZ depois de clonar/criar o projeto no NetBeans.
# =============================================================================

$origem  = "C:\Users\ruanp\NETBENAS - COWORK\migration-engine-jframe\migration-engine\lib"
$destino = "$PSScriptRoot\lib"

if (-not (Test-Path $destino)) {
    New-Item -ItemType Directory -Path $destino | Out-Null
    Write-Host "Pasta lib criada: $destino"
}

$arquivos = Get-ChildItem -Path $origem -Filter "*.jar"
foreach ($jar in $arquivos) {
    $dst = Join-Path $destino $jar.Name
    if (-not (Test-Path $dst)) {
        Copy-Item $jar.FullName -Destination $dst
        Write-Host "Copiado: $($jar.Name)"
    } else {
        Write-Host "Ja existe: $($jar.Name)"
    }
}

Write-Host ""
Write-Host "Pronto! $($arquivos.Count) JARs disponiveis em $destino"
