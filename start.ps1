##############################################################
#  BJJ Search Engine - Start Script
#  Starts: Docker (Postgres + Qdrant), Backend, Frontend
#  Usage:  .\start.ps1
#          .\start.ps1 -NoBrowser   (skip auto-open browser)
##############################################################
param(
    [switch]$NoBrowser
)

$ROOT     = "C:\Users\Skeez\Documents\chunking"
$BACKEND  = "$ROOT\backend"
$FRONTEND = "$ROOT\frontend"
$LOGS     = "$ROOT\logs"

New-Item -ItemType Directory -Force -Path $LOGS | Out-Null

# -- Helpers ---------------------------------------------------
function Write-Step($n, $msg) {
    Write-Host ""
    Write-Host "  [$n] $msg" -ForegroundColor Cyan
}
function Write-OK($msg)   { Write-Host "      OK  $msg" -ForegroundColor Green }
function Write-Wait($msg) { Write-Host "      ... $msg" -ForegroundColor Yellow }
function Write-Fail($msg) { Write-Host "      ERR $msg" -ForegroundColor Red }

function Wait-Http($url, $label, $timeoutSec = 60) {
    $waited = 0
    while ($waited -lt $timeoutSec) {
        try {
            $null = Invoke-WebRequest -Uri $url -TimeoutSec 2 -UseBasicParsing -ErrorAction Stop
            Write-OK "$label is ready"
            return $true
        } catch {}
        Start-Sleep -Seconds 2
        $waited += 2
        Write-Wait "Waiting for $label... ${waited}s"
    }
    Write-Fail "$label did not respond after ${timeoutSec}s"
    return $false
}

function Test-Port($port) {
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect("127.0.0.1", $port)
        $tcp.Close()
        return $true
    } catch { return $false }
}

# -- Banner ----------------------------------------------------
Clear-Host
Write-Host ""
Write-Host "  ==========================================================" -ForegroundColor White
Write-Host "    BJJ Instructional Search Engine" -ForegroundColor White
Write-Host "    $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor DarkGray
Write-Host "  ==========================================================" -ForegroundColor White

# -- 1. Docker - Postgres + Qdrant ----------------------------
Write-Step "1/4" "Starting Docker services (Postgres + Qdrant)..."

try {
    $null = docker info 2>&1
} catch {
    Write-Fail "Docker is not running. Please start Docker Desktop first."
    exit 1
}

Set-Location $ROOT
docker compose up -d db qdrant | Out-Null

# Wait for Postgres
$pgReady = $false
$waited  = 0
while ($waited -lt 30) {
    $check = docker compose exec -T db pg_isready -U bjj -d bjj_search 2>&1
    if ($check -match "accepting connections") { $pgReady = $true; break }
    Start-Sleep -Seconds 2; $waited += 2
    Write-Wait "Waiting for Postgres... ${waited}s"
}
if ($pgReady) { Write-OK "Postgres ready (port 5432)" }
else          { Write-Fail "Postgres did not start - check Docker Desktop" }

# Wait for Qdrant
if (Wait-Http "http://localhost:6333/healthz" "Qdrant" 30) {}

# -- 2. Backend - FastAPI / uvicorn ---------------------------
Write-Step "2/4" "Starting backend (FastAPI on :8000)..."

$stale = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue
if ($stale) {
    $stale | ForEach-Object {
        Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
}

$backendLog = "$LOGS\backend.log"
"" | Set-Content $backendLog

Start-Process pwsh -ArgumentList @(
    "-NoProfile", "-WindowStyle", "Minimized", "-Command",
    "Set-Location '$BACKEND'; " +
    "python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 *>> '$backendLog'"
) -PassThru | Out-Null

$backendOK = Wait-Http "http://localhost:8000/api/health" "Backend" 60

# -- 3. Frontend - Next.js dev server -------------------------
Write-Step "3/4" "Starting frontend (Next.js on :3000)..."

$stale3 = Get-NetTCPConnection -LocalPort 3000 -State Listen -ErrorAction SilentlyContinue
if ($stale3) {
    $stale3 | ForEach-Object {
        Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
}

$frontendLog = "$LOGS\frontend.log"
"" | Set-Content $frontendLog

Start-Process pwsh -ArgumentList @(
    "-NoProfile", "-WindowStyle", "Minimized", "-Command",
    "Set-Location '$FRONTEND'; npm run dev *>> '$frontendLog'"
) -PassThru | Out-Null

$waited     = 0
$frontendOK = $false
while ($waited -lt 60) {
    Start-Sleep -Seconds 2; $waited += 2
    $logContent = Get-Content $frontendLog -ErrorAction SilentlyContinue
    if ($logContent -match "Ready in|ready on|Local:.*3000") {
        $frontendOK = $true
        Write-OK "Frontend ready (port 3000)"
        break
    }
    if (Test-Port 3000) {
        $frontendOK = $true
        Write-OK "Frontend ready (port 3000)"
        break
    }
    Write-Wait "Waiting for Next.js... ${waited}s"
}
if (-not $frontendOK) { Write-Fail "Frontend did not start - check $frontendLog" }

# -- 4. Summary -----------------------------------------------
Write-Host ""
Write-Host "  ==========================================================" -ForegroundColor White
Write-Host "    All services started" -ForegroundColor Green
Write-Host "  ==========================================================" -ForegroundColor White
Write-Host ""
Write-Host "    App         http://localhost:3000" -ForegroundColor White
Write-Host "    Backend API http://localhost:8000/docs" -ForegroundColor White
Write-Host "    Qdrant UI   http://localhost:6333/dashboard" -ForegroundColor White
Write-Host ""
Write-Host "    Logs" -ForegroundColor DarkGray
Write-Host "      Backend  : $backendLog" -ForegroundColor DarkGray
Write-Host "      Frontend : $frontendLog" -ForegroundColor DarkGray
Write-Host ""
Write-Host "    To stop all services:" -ForegroundColor DarkGray
Write-Host "      .\stop.ps1" -ForegroundColor DarkGray
Write-Host ""

# -- Open browser ---------------------------------------------
if (-not $NoBrowser -and $frontendOK) {
    Start-Process "http://localhost:3000"
}
