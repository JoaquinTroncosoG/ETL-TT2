@echo off
setlocal
:: Cambiar al directorio raiz del proyecto
cd /d "%~dp0\.."

:: Verificar y activar el entorno virtual
if exist "venv\Scripts\activate.bat" (
    call "venv\Scripts\activate.bat"
)

:: Ejecutar el script batch y capturar codigo de retorno
python "tools\ejecutar_batch.py" %*
set EXIT_CODE=%ERRORLEVEL%

exit /b %EXIT_CODE%
