# =============================================================================
#  OCI — Retry automatico de criacao da VM ARM A1.Flex
#  Stack: instance-migracao (4 OCPU / 24 GB RAM / Ubuntu 22.04)
#  Executa a cada 60 segundos ate conseguir ou voce fechar a janela.
# =============================================================================

$STACK_ID  = "ocid1.ormstack.oc1.sa-saopaulo-1.amaaaaaajva7jgiaibbth4bgw3dpckzqrfh4263hsj5li3sual6uconqihoq"
$OCI       = "C:\oci\Scripts\oci"
$LOG_FILE  = Join-Path $PSScriptRoot "retry.log"
$MAX_LOG   = 200   # linhas maximas no log (evita crescer demais)

# ---------------------------------------------------------------------------
function Write-Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $msg"
    Add-Content $LOG_FILE $line
    Write-Host $line
}

function Trim-Log {
    if (-not (Test-Path $LOG_FILE)) { return }
    $lines = Get-Content $LOG_FILE
    if ($lines.Count -gt $MAX_LOG) {
        $lines | Select-Object -Last $MAX_LOG | Set-Content $LOG_FILE
    }
}

function Get-JobState($jobId) {
    $raw = & $OCI resource-manager job get --job-id $jobId 2>&1
    $match = ($raw | Select-String '"lifecycle-state"\s*:\s*"(\w+)"' | Select-Object -First 1)
    if ($match) { return $match.Matches.Groups[1].Value }
    return ""
}

function Get-JobId($raw) {
    $match = ($raw | Select-String '"id"\s*:\s*"(ocid1\.ormjob[^"]+)"' | Select-Object -First 1)
    if ($match) { return $match.Matches.Groups[1].Value }
    return ""
}

# ---------------------------------------------------------------------------
#  Verifica se VM ja existe antes de comecar
# ---------------------------------------------------------------------------
function Check-VmExiste {
    $raw = & $OCI compute instance list `
        --compartment-id "ocid1.tenancy.oc1..aaaaaaaa2twsotmluyxdws7urczxdbyddaax2flp5broi2w4i74xkf2hg2qa" 2>&1
    if ($raw -match '"display-name".*"instance-migracao"') {
        $state = ($raw | Select-String '"lifecycle-state"\s*:\s*"(\w+)"' | Select-Object -First 1).Matches.Groups[1].Value
        if ($state -and $state -ne "TERMINATED") { return $state }
    }
    return ""
}

# ---------------------------------------------------------------------------
#  INICIO
# ---------------------------------------------------------------------------
Clear-Host
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "  OCI Retry — VM.Standard.A1.Flex (24 GB RAM)  " -ForegroundColor Cyan
Write-Host "  Stack: instance-migracao / sa-saopaulo-1     " -ForegroundColor Cyan
Write-Host "  Log: $LOG_FILE" -ForegroundColor DarkGray
Write-Host "  Pressione Ctrl+C para parar                  " -ForegroundColor DarkGray
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host ""

# Verifica se VM ja foi criada
$vmState = Check-VmExiste
if ($vmState) {
    Write-Host "✅ VM ja existe com estado: $vmState" -ForegroundColor Green
    Write-Host "   Acesse: Compute -> Instances -> instance-migracao" -ForegroundColor Green
    Write-Log "VM ja existe (state=$vmState) — script encerrado."
    Read-Host "Pressione Enter para sair"
    exit 0
}

Write-Log "=== Iniciando retry loop ==="
Trim-Log

$tentativa = 0

while ($true) {
    $tentativa++
    Write-Host ""
    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Tentativa #$tentativa — disparando Apply..." -ForegroundColor Yellow -NoNewline

    # Dispara Apply no Stack
    $raw   = & $OCI resource-manager job create-apply-job `
                 --stack-id $STACK_ID `
                 --execution-plan-strategy AUTO_APPROVED 2>&1
    $jobId = Get-JobId ($raw -join "`n")

    if (-not $jobId) {
        Write-Host " ERRO ao criar job" -ForegroundColor Red
        Write-Log "#$tentativa ERRO ao criar job"
        Start-Sleep -Seconds 60
        continue
    }

    Write-Host " Job criado" -ForegroundColor Yellow
    Write-Host "  JobId: $($jobId.Substring(0, [Math]::Min(50,$jobId.Length)))..."

    # Monitora o job (max 5 minutos)
    $sucesso = $false
    for ($w = 0; $w -lt 60; $w++) {
        Start-Sleep -Seconds 5
        $state = Get-JobState $jobId

        switch ($state) {
            "SUCCEEDED" {
                Write-Host ""
                Write-Host "  ✅ INSTANCIA CRIADA COM SUCESSO!" -ForegroundColor Green
                Write-Host "  Acesse: Compute -> Instances -> instance-migracao" -ForegroundColor Green
                Write-Log "#$tentativa SUCCEEDED — VM criada!"
                $sucesso = $true
                break
            }
            "FAILED" {
                Write-Host "  FAILED — sem capacidade" -ForegroundColor DarkRed
                Write-Log "#$tentativa FAILED"
                break
            }
            "" {
                Write-Host "  timeout/erro ao checar estado" -ForegroundColor DarkRed
                Write-Log "#$tentativa timeout"
                break
            }
            default {
                Write-Host "  . $state" -ForegroundColor DarkGray -NoNewline
            }
        }

        if ($state -eq "SUCCEEDED" -or $state -eq "FAILED" -or $state -eq "") { break }
    }

    if ($sucesso) {
        Trim-Log
        Write-Host ""
        Write-Host "Pressione Enter para sair..."
        Read-Host
        exit 0
    }

    # Limpa log se muito grande
    Trim-Log

    Write-Host "  Aguardando 60s antes da proxima tentativa..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 60
}
