# ============================================================================
# PROGRAMADOR DE TAREAS DE WINDOWS (TASK SCHEDULER) - PROCESO BATCH ETL
# Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)
# ============================================================================

param (
    [Parameter(Mandatory=$false)]
    [ValidateSet("crear", "ver", "ejecutar", "eliminar")]
    [string]$Accion = "crear",

    [Parameter(Mandatory=$false)]
    [string]$Hora = "00:00"
)

$NombreTarea = "ETL_Multiplataforma_Batch"
$RaizProyecto = Resolve-Path (Join-Path $PSScriptRoot "..")
$RunnerCmd = Join-Path $RaizProyecto "tools\ejecutar_batch.cmd"

if (-not (Test-Path $RunnerCmd)) {
    Write-Error "[!] No se encontro el archivo runner en: $RunnerCmd"
    exit 1
}

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  ADMINISTRADOR DE TAREA PROGRAMADA BATCH (WINDOWS)" -ForegroundColor Cyan
Write-Host "  Proyecto: $RaizProyecto" -ForegroundColor Gray
Write-Host "  Runner:   $RunnerCmd" -ForegroundColor Gray
Write-Host "=================================================================" -ForegroundColor Cyan

switch ($Accion) {
    "crear" {
        Write-Host "`nCreando tarea programada '$NombreTarea' para las $Hora hrs diarias..." -ForegroundColor Yellow
        $Comando = "`"$RunnerCmd`""
        
        # Crear la tarea programada con schtasks
        schtasks /create /tn $NombreTarea /tr $Comando /sc daily /st $Hora /f | Out-Host
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "`n[OK] Tarea programada registrada exitosamente en Windows." -ForegroundColor Green
            Write-Host "     Se ejecutara todos los dias a las $Hora hrs de forma desatendida." -ForegroundColor Green
            Write-Host "     Los logs quedaran guardados en: $RaizProyecto\logs\" -ForegroundColor Gray
        } else {
            Write-Error "[!] Error registrando la tarea en Windows."
        }
    }

    "ver" {
        Write-Host "`nConsultando estado de la tarea '$NombreTarea'..." -ForegroundColor Yellow
        schtasks /query /tn $NombreTarea /v /fo list | Out-Host
    }

    "ejecutar" {
        Write-Host "`nDisparando ejecucion manual inmediata de la tarea '$NombreTarea'..." -ForegroundColor Yellow
        schtasks /run /tn $NombreTarea | Out-Host
        Write-Host "[i] Tarea iniciada en segundo plano. Puedes revisar logs en: $RaizProyecto\logs\ultimo_batch.log" -ForegroundColor Cyan
    }

    "eliminar" {
        Write-Host "`nEliminando tarea programada '$NombreTarea'..." -ForegroundColor Yellow
        schtasks /delete /tn $NombreTarea /f | Out-Host
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[OK] Tarea '$NombreTarea' eliminada correctamente de Windows." -ForegroundColor Green
        }
    }
}
