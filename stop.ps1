##############################################################
#  BJJ Search Engine — Stop Script
##############################################################

$ROOT = "C:\Users\Skeez\Documents\chunking"

Write-Host ""
Write-Host "  Stopping BJJ Search Engine..." -ForegroundColor Cyan
Write-Host ""

# Kill uvicorn / backend on port 8000
$p8000 = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue
if ($p8000) {
    $p8000 | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }
    Write-Host "  Stopped backend (port 8000)" -ForegroundColor Green
} else {
    Write-Host "  Backend not running" -ForegroundColor DarkGray
}

# Kill Next.js / frontend on port 3000
$p3000 = Get-NetTCPConnection -LocalPort 3000 -State Listen -ErrorAction SilentlyContinue
if ($p3000) {
    $p3000 | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }
    Write-Host "  Stopped frontend (port 3000)" -ForegroundColor Green
} else {
    Write-Host "  Frontend not running" -ForegroundColor DarkGray
}

# Stop Docker services
Set-Location $ROOT
docker compose stop db qdrant 2>&1 | Out-Null
Write-Host "  Stopped Docker services (Postgres + Qdrant)" -ForegroundColor Green

Write-Host ""
Write-Host "  All services stopped." -ForegroundColor White
Write-Host ""
