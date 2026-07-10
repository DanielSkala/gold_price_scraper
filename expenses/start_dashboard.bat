@echo off
REM Start the Expense Analytics dashboard on Windows.
REM On macOS / Linux, use start_dashboard.sh instead.

cd /d "%~dp0"

echo Starting Expense Analytics Dashboard...
echo.

if not exist app.py (
    echo Error: app.py not found ^(run this from the expenses\ directory^).
    exit /b 1
)

REM Prefer uv (installs deps + runs in one step); fall back to py, then python.
set "RUN="
where uv >nul 2>nul && set "RUN=uv run app.py"
if not defined RUN where py >nul 2>nul && set "RUN=py app.py"
if not defined RUN where python >nul 2>nul && set "RUN=python app.py"

if not defined RUN (
    echo Error: neither uv nor Python 3 found.
    echo Install uv ^(https://docs.astral.sh/uv/^) or Python ^(https://www.python.org/downloads/^).
    exit /b 1
)

echo Dashboard will be available at: http://localhost:5001
echo Press Ctrl+C to stop the server.
echo.

%RUN%
