@echo off
REM Start the Expense Analytics dashboard on Windows.
REM On macOS / Linux, use start_dashboard.sh instead.

cd /d "%~dp0"

echo Starting Expense Analytics Dashboard...
echo.

REM Prefer the "py" launcher, fall back to python.
where py >nul 2>nul
if %ERRORLEVEL%==0 (
    set PY=py
) else (
    where python >nul 2>nul
    if %ERRORLEVEL%==0 (
        set PY=python
    ) else (
        echo Error: Python 3 not found. Install it from https://www.python.org/downloads/
        exit /b 1
    )
)

if not exist app.py (
    echo Error: app.py not found ^(run this from the expenses\ directory^).
    exit /b 1
)

echo Dashboard will be available at: http://localhost:5001
echo Press Ctrl+C to stop the server.
echo.

%PY% app.py
