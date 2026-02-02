@echo off
REM BSK Data Sync - Windows Batch Script

REM Set paths (ADJUST THESE TO YOUR ACTUAL PATHS!)
set PROJECT_DIR=C:\Users\YourName\Projects\bsk-training-api
set PYTHON_EXE=C:\Users\YourName\Projects\bsk-training-api\venv\Scripts\python.exe
set SCRIPT_PATH=%PROJECT_DIR%\scripts\cron_sync.py

REM Change to project directory
cd /d %PROJECT_DIR%

REM Run sync script
echo Starting BSK Data Sync...
%PYTHON_EXE% %SCRIPT_PATH% --table all

REM Check if successful
if %ERRORLEVEL% EQU 0 (
    echo Sync completed successfully!
) else (
    echo Sync failed with error code %ERRORLEVEL%
)

pause