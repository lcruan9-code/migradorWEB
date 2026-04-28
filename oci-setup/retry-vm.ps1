# ============================================================
#  OCI Retry - VM.Standard.A1.Flex
#  Faz PLAN uma vez (valida config), depois fica tentando
#  APPLY a cada 5 minutos ate criar a VM ou Ctrl+C.
# ============================================================

$STACK_ID     = "ocid1.ormstack.oc1.sa-saopaulo-1.amaaaaaajva7jgiaibbth4bgw3dpckzqrfh4263hsj5li3sual6uconqihoq"
$COMPARTMENT  = "ocid1.tenancy.oc1..aaaaaaaa2twsotmluyxdws7urczxdbyddaax2flp5broi2w4i74xkf2hg2qa"
$OCI          = "C:\oci\Scripts\oci"
$LOG_FILE     = Join-Path $PSScriptRoot "retry.log"
$MAX_LOG      = 300
$WAIT_SECS    = 300   # 5 minutos entre tentativas

# ── Helpers ──────────────────────────────────────────────────

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

function Get-Field($text, $field) {
    $m = [regex]::Match($text, "`"$field`"\s*:\s*`"([^`"]+)`"")
    if ($m.Success) { return $m.Groups[1].Value }
    return ""
}

function Get-JobId($text) {
    $m = [regex]::Match($text, '"id"\s*:\s*"(ocid1\.ormjob[^"]+)"')
    if ($m.Success) { return $m.Groups[1].Value }
    return ""
}

function Get-JobStatus($jobId) {
    $raw = (& $OCI resource-manager job get --job-id $jobId 2>&1) -join "`n"
    return Get-Field $raw "lifecycle-state"
}

function Get-FailureMsg($jobId) {
    $raw = (& $OCI resource-manager job get --job-id $jobId 2>&1) -join "`n"
    $msg = Get-Field $raw "message"
    if (-not $msg) {
        # Tenta pegar logs do job para encontrar "Error:"
        $logs = (& $OCI resource-manager job get-job-logs-content --job-id $jobId --query "data" --raw-output 2>&1) -join "`n"
        $m = [regex]::Match($logs, 'Error:\s*(.+)')
        if ($m.Success) { $msg = $m.Groups[1].Value.Trim() }
    }
    return $msg
}

function Wait-ForJob($jobId, $tipoJob) {
    # Monitora o job mostrando pontos ate terminar
    # Retorna: "SUCCEEDED", "FAILED" ou "TIMEOUT"
    Write-Host -NoNewline "  Status $tipoJob`:"
    $prevStatus = ""
    for ($w = 0; $w -lt 120; $w++) {   # max 10 min (120 x 5s)
        Start-Sleep -Seconds 5
        $status = Get-JobStatus $jobId
        if ($status -ne $prevStatus -and $status -ne "") {
            Write-Host -NoNewline " $status"
            $prevStatus = $status
        } else {
            Write-Host -NoNewline "." -ForegroundColor DarkGray
        }
        if ($status -eq "SUCCEEDED") { Write-Host ""; return "SUCCEEDED" }
        if ($status -eq "FAILED")    { Write-Host ""; return "FAILED" }
    }
    Write-Host " TIMEOUT"
    return "TIMEOUT"
}

function Check-VmExiste {
    $raw = (& $OCI compute instance list --compartment-id $COMPARTMENT 2>&1) -join "`n"
    if ($raw -match '"display-name"[^}]*"instance-migracao"') {
        $st = Get-Field $raw "lifecycle-state"
        if ($st -and $st -ne "TERMINATED") { return $st }
    }
    return ""
}

# ── Banner ───────────────────────────────────────────────────

Clear-Host
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "  OCI Retry - VM.Standard.A1.Flex               " -ForegroundColor Cyan
Write-Host "  Stack: instance-migracao / sa-saopaulo-1      " -ForegroundColor Cyan
Write-Host "  Log: $LOG_FILE" -ForegroundColor DarkGray
Write-Host "  Pressione Ctrl+C para parar                   " -ForegroundColor DarkGray
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host ""

# ── Verifica se VM ja existe ─────────────────────────────────

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

# ── PLAN (uma vez) — valida o config Terraform ───────────────

Write-Host ""
Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Executando PLAN para validar configuracao..." -ForegroundColor Cyan
Write-Log "Iniciando PLAN..."

$planRaw = (& $OCI resource-manager job create --stack-id $STACK_ID --operation PLAN 2>&1) -join "`n"
$planJobId = Get-JobId $planRaw

if (-not $planJobId) {
    Write-Log "ERRO ao criar job PLAN: $planRaw"
    Write-Host "ERRO ao criar job PLAN. Verifique o Stack ID e tente novamente." -ForegroundColor Red
    Read-Host "Pressione Enter para sair"
    exit 1
}

Write-Log "Job PLAN criado: $planJobId"
$planResult = Wait-ForJob $planJobId "PLAN"

if ($planResult -ne "SUCCEEDED") {
    $errMsg = Get-FailureMsg $planJobId
    Write-Log "PLAN $planResult`: $errMsg"
    Write-Host ""
    Write-Host "PLAN falhou ($planResult) — erro de configuracao no Terraform." -ForegroundColor Red
    Write-Host "Mensagem: $errMsg" -ForegroundColor Red
    Write-Host "Corrija o Stack antes de continuar." -ForegroundColor Yellow
    Read-Host "Pressione Enter para sair"
    exit 1
}

Write-Host "  Configuracao validada com sucesso!" -ForegroundColor Green
Write-Log "PLAN SUCCEEDED - config OK"

# ── Loop APPLY ───────────────────────────────────────────────

$tentativa = 0
while ($true) {
    $tentativa++
    Write-Host ""
    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Tentativa #$tentativa - criando APPLY job..." -ForegroundColor Yellow

    $applyRaw = (& $OCI resource-manager job create-apply-job --stack-id $STACK_ID --execution-plan-strategy AUTO_APPROVED 2>&1) -join "`n"
    $applyJobId = Get-JobId $applyRaw

    if (-not $applyJobId) {
        Write-Log "#$tentativa ERRO ao criar job APPLY (rate limit ou API error)"
        Write-Host "  ERRO ao criar job — possivel rate limit. Aguardando $($WAIT_SECS/60) min..." -ForegroundColor Red
        Start-Sleep -Seconds $WAIT_SECS
        continue
    }

    Write-Log "#$tentativa APPLY job criado: $applyJobId"
    $applyResult = Wait-ForJob $applyJobId "APPLY"

    if ($applyResult -eq "SUCCEEDED") {
        # Confirma VM criada
        $ipRaw = (& $OCI compute instance list --compartment-id $COMPARTMENT --display-name "instance-migracao" 2>&1) -join "`n"
        $pubIp = Get-Field $ipRaw "public-ip"
        Write-Host ""
        Write-Host "  ============================================" -ForegroundColor Green
        Write-Host "  VM CRIADA COM SUCESSO!" -ForegroundColor Green
        if ($pubIp) { Write-Host "  IP Publico: $pubIp" -ForegroundColor Green }
        Write-Host "  Acesse: Compute -> Instances -> instance-migracao" -ForegroundColor Green
        Write-Host "  ============================================" -ForegroundColor Green
        Write-Log "#$tentativa SUCCEEDED - VM criada! IP=$pubIp"
        Trim-Log
        Read-Host "Pressione Enter para sair"
        exit 0
    }

    # FAILED — pega a mensagem real de erro
    $errMsg = Get-FailureMsg $applyJobId
    if (-not $errMsg) { $errMsg = "sem capacidade ou erro desconhecido" }
    Write-Host "  FAILED: $errMsg" -ForegroundColor DarkRed
    Write-Log "#$tentativa FAILED: $errMsg"

    Trim-Log
    Write-Host "  Aguardando $($WAIT_SECS/60) min para proxima tentativa..." -ForegroundColor DarkGray
    Start-Sleep -Seconds $WAIT_SECS
}
