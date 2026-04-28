$STACK_ID = "ocid1.ormstack.oc1.sa-saopaulo-1.amaaaaaajva7jgiaibbth4bgw3dpckzqrfh4263hsj5li3sual6uconqihoq"
$OCI      = "C:\oci\Scripts\oci"
$LOG_FILE = Join-Path $PSScriptRoot "retry.log"
$MAX_LOG  = 200

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
    $m = ($raw | Select-String '"lifecycle-state"\s*:\s*"(\w+)"' | Select-Object -First 1)
    if ($m) { return $m.Matches.Groups[1].Value }
    return ""
}

function Get-JobId($raw) {
    $m = ($raw | Select-String '"id"\s*:\s*"(ocid1\.ormjob[^"]+)"' | Select-Object -First 1)
    if ($m) { return $m.Matches.Groups[1].Value }
    return ""
}

function Check-VmExiste {
    $raw = & $OCI compute instance list --compartment-id "ocid1.tenancy.oc1..aaaaaaaa2twsotmluyxdws7urczxdbyddaax2flp5broi2w4i74xkf2hg2qa" 2>&1
    if ($raw -match '"display-name".*"instance-migracao"') {
        $m = ($raw | Select-String '"lifecycle-state"\s*:\s*"(\w+)"' | Select-Object -First 1)
        if ($m) {
            $st = $m.Matches.Groups[1].Value
            if ($st -and $st -ne "TERMINATED") { return $st }
        }
    }
    return ""
}

Clear-Host
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "  OCI Retry - VM.Standard.A1.Flex (24 GB RAM)  " -ForegroundColor Cyan
Write-Host "  Stack: instance-migracao / sa-saopaulo-1     " -ForegroundColor Cyan
Write-Host "  Log: $LOG_FILE" -ForegroundColor DarkGray
Write-Host "  Pressione Ctrl+C para parar                  " -ForegroundColor DarkGray
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host ""

$vmState = Check-VmExiste
if ($vmState) {
    Write-Host "VM ja existe com estado: $vmState" -ForegroundColor Green
    Write-Host "Acesse: Compute -> Instances -> instance-migracao" -ForegroundColor Green
    Write-Log "VM ja existe (state=$vmState) - script encerrado."
    Read-Host "Pressione Enter para sair"
    exit 0
}

Write-Log "=== Iniciando retry loop ==="
Trim-Log

$tentativa = 0
while ($true) {
    $tentativa++
    Write-Host ""
    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Tentativa #$tentativa - disparando Apply..." -ForegroundColor Yellow -NoNewline

    $raw   = & $OCI resource-manager job create-apply-job --stack-id $STACK_ID --execution-plan-strategy AUTO_APPROVED 2>&1
    $jobId = Get-JobId ($raw -join "`n")

    if (-not $jobId) {
        Write-Host " ERRO ao criar job" -ForegroundColor Red
        Write-Log "#$tentativa ERRO ao criar job"
        Start-Sleep -Seconds 60
        continue
    }

    Write-Host " Job criado" -ForegroundColor Yellow
    $jobShort = $jobId.Substring(0, [Math]::Min(55, $jobId.Length))
    Write-Host "  JobId: $jobShort..."

    $sucesso = $false
    for ($w = 0; $w -lt 60; $w++) {
        Start-Sleep -Seconds 5
        $state = Get-JobState $jobId
        if ($state -eq "SUCCEEDED") {
            Write-Host ""
            Write-Host "  VM CRIADA COM SUCESSO!" -ForegroundColor Green
            Write-Host "  Acesse: Compute -> Instances -> instance-migracao" -ForegroundColor Green
            Write-Log "#$tentativa SUCCEEDED - VM criada!"
            $sucesso = $true
            break
        }
        if ($state -eq "FAILED" -or $state -eq "") {
            Write-Host "  FAILED - sem capacidade" -ForegroundColor DarkRed
            Write-Log "#$tentativa FAILED"
            break
        }
        Write-Host "." -NoNewline -ForegroundColor DarkGray
    }

    if ($sucesso) {
        Trim-Log
        Write-Host ""
        Read-Host "Pressione Enter para sair"
        exit 0
    }

    Trim-Log
    Write-Host "  Aguardando 60s..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 60
}