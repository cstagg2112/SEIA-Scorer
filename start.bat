@echo off
echo.
echo SEIA M&A Scorer — setup
echo ========================

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Install from https://python.org
    pause
    exit /b 1
)

if not exist venv (
    echo Creating virtual environment...
    python -m venv venv
)

echo Installing dependencies...
call venv\Scripts\activate
pip install -q -r requirements.txt

echo.
echo Starting server...
echo Open http://localhost:8000 in your browser
echo Press Ctrl+C to stop
echo.

uvicorn main:app --host 0.0.0.0 --port 8000 --reload
pause
