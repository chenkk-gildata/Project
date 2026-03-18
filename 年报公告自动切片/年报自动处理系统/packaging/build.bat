@echo off
setlocal enabledelayedexpansion

echo ========================================
echo Annual Report Auto Processing System - Build Script
echo ========================================
echo.

REM Get script directory and project root
set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."

REM Change to project root directory
cd /d "%PROJECT_ROOT%"

echo [INFO] Project root: %cd%
echo.

REM Check Python environment
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found, please install Python first
    pause
    exit /b 1
)

REM Check PyInstaller
python -c "import PyInstaller" >nul 2>&1
if errorlevel 1 (
    echo [INFO] PyInstaller not installed, installing...
    pip install pyinstaller
    if errorlevel 1 (
        echo [ERROR] PyInstaller installation failed
        pause
        exit /b 1
    )
)

echo [INFO] Building main program...
echo.
pyinstaller "%SCRIPT_DIR%main.spec" --clean
if errorlevel 1 (
    echo [ERROR] Main program build failed
    pause
    exit /b 1
)

echo.
echo [INFO] Building tools program...
echo.
pyinstaller "%SCRIPT_DIR%tools.spec" --clean
if errorlevel 1 (
    echo [ERROR] Tools program build failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo Build completed!
echo ========================================
echo.
echo Output directory: %cd%\dist
echo.
echo Generated executables:
echo   - AnnualReportProcessor.exe
echo   - StatusViewer.exe
echo.
echo Instructions:
echo   1. Copy the exe files from dist folder to target location
echo   2. Make sure the target location has the following structure:
echo      - storage\ (for database and logs)
echo      - Annual Report\raw\ (for downloaded PDFs)
echo   3. Required directories will be created automatically on first run
echo.
pause
