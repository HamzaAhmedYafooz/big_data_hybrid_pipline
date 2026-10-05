@echo off
title Big Data Pipeline - Al-Razi University
echo.
echo =================================================================
echo   Big Data Pipeline Project (Al-Razi University)
echo   Starting FastAPI Server + Dashboard
echo =================================================================
echo.

REM -- Detect Python (prefer the one in PATH, fall back to common locations) --
where python >nul 2>&1
if %ERRORLEVEL% == 0 (
    set PYTHON=python
) else if exist "C:\Users\AL MASA\AppData\Local\Programs\Python\Python312\python.exe" (
    set PYTHON="C:\Users\AL MASA\AppData\Local\Programs\Python\Python312\python.exe"
) else (
    echo [ERROR] Python not found. Please install Python 3.10+ and add it to PATH.
    pause
    exit /b 1
)

REM -- Move to the project directory (same folder as this .bat file) --
cd /d "%~dp0"

echo [1/3] Checking required packages...
%PYTHON% -m pip install -q fastapi uvicorn pymongo pydantic
echo       Done.
echo.

echo [2/3] Launching FastAPI server on http://127.0.0.1:8000 ...
echo       Dashboard : http://127.0.0.1:8000/dashboard
echo       Swagger   : http://127.0.0.1:8000/docs
echo       Health    : http://127.0.0.1:8000/health
echo.

REM -- Small delay so the server binds before the browser opens --
start "" cmd /c "timeout /t 3 /nobreak >nul && start http://127.0.0.1:8000/dashboard"

echo [3/3] Press CTRL+C to stop the server.
echo.
%PYTHON% -m uvicorn src.api:app --host 127.0.0.1 --port 8000 --reload

pause
