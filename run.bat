@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

REM ============================================================================
REM   GrabTube - Windows launcher
REM
REM   Double-click this file. First run:
REM     1. checks for Python 3.10+
REM     2. creates a .venv folder in this directory
REM     3. pip installs requirements.txt into that venv (nothing global)
REM     4. downloads portable ffmpeg into .tools\ (nothing system-wide)
REM     5. launches the app and opens your browser
REM
REM   Every run after the first is instant - setup is cached behind a marker.
REM
REM   Flags:
REM     --reinstall     force a clean setup
REM     --port N        serve on port N        (default 8000)
REM     --host ADDR     bind to ADDR           (default 127.0.0.1)
REM     --no-browser    don't auto-open the browser
REM     --skip-ffmpeg   don't download ffmpeg
REM
REM   Any other flag is passed through to app.py.
REM ============================================================================

REM ---- args -----------------------------------------------------------------
set "REINSTALL=0"
set "NO_BROWSER=0"
set "SKIP_FFMPEG=0"
set "PORT=8000"
set "BIND_HOST=127.0.0.1"
set "PASSTHRU="

:parse_args
if "%~1"=="" goto :args_done
if /I "%~1"=="--reinstall"   ( set "REINSTALL=1"   & shift & goto :parse_args )
if /I "%~1"=="--no-browser"  ( set "NO_BROWSER=1"  & shift & goto :parse_args )
if /I "%~1"=="--skip-ffmpeg" ( set "SKIP_FFMPEG=1" & shift & goto :parse_args )
if /I "%~1"=="--port"        ( set "PORT=%~2"      & shift & shift & goto :parse_args )
if /I "%~1"=="--host"        ( set "BIND_HOST=%~2" & shift & shift & goto :parse_args )
set "PASSTHRU=!PASSTHRU! %~1"
shift
goto :parse_args
:args_done

REM ---- banner ---------------------------------------------------------------
echo.
echo   =====================================================
echo     GrabTube
echo     self-hosted . runs locally . no accounts
echo   =====================================================
echo.

if not exist "app.py" (
    echo   [x] app.py not found in this folder
    echo       Make sure run.bat sits next to app.py
    echo.
    pause
    exit /b 1
)

if "%REINSTALL%"=="1" (
    echo   . --reinstall: clearing setup cache
    if exist ".venv\.installed" del /q ".venv\.installed" >nul 2>&1
)

REM ---- 1. Python ------------------------------------------------------------
echo.
echo   [1] Checking for Python 3.10+

set "PY="
python --version >nul 2>&1
if not errorlevel 1 set "PY=python"
if not defined PY (
    py -3 --version >nul 2>&1
    if not errorlevel 1 set "PY=py -3"
)

if not defined PY goto :no_python

%PY% -c "import sys; sys.exit(0 if sys.version_info[:2] >= (3,10) else 1)" >nul 2>&1
if errorlevel 1 goto :bad_python

echo       v found:
%PY% --version

REM ---- 2+3. venv + deps -----------------------------------------------------
if exist ".venv\Scripts\python.exe" if exist ".venv\.installed" goto :already_setup

echo.
echo   [2] Setting up virtual environment

if exist ".venv\Scripts\python.exe" goto :venv_ok
echo       . creating .venv
%PY% -m venv .venv
if not exist ".venv\Scripts\python.exe" (
    echo       x venv creation failed
    echo.
    pause
    exit /b 1
)
:venv_ok
echo       v .venv ready

echo.
echo   [3] Installing dependencies
echo       . pip install -r requirements.txt

".venv\Scripts\python.exe" -m pip install --upgrade pip --quiet
if errorlevel 1 goto :pip_failed

".venv\Scripts\python.exe" -m pip install -r requirements.txt --quiet
if errorlevel 1 goto :pip_failed

echo       v fastapi . uvicorn . yt-dlp . websockets . pydantic
echo setup %DATE% %TIME%> ".venv\.installed"
echo       v setup cached - future launches will be instant
goto :skip_setup

:already_setup
echo.
echo   [2] Environment already set up
echo       v .venv cached - launching directly

:skip_setup

REM ---- 4. ffmpeg ------------------------------------------------------------
echo.
echo   [4] Checking for ffmpeg

where ffmpeg >nul 2>&1
if not errorlevel 1 (
    echo       v ffmpeg on PATH
    goto :ffmpeg_done
)

if "%SKIP_FFMPEG%"=="1" (
    echo       ! skipping ffmpeg - merging will fail
    goto :ffmpeg_done
)

if exist ".tools\ffmpeg\bin\ffmpeg.exe" (
    set "PATH=%CD%\.tools\ffmpeg\bin;!PATH!"
    echo       v using portable ffmpeg from .tools
    goto :ffmpeg_done
)

echo       . not found - downloading portable build
echo         ~100 MB, one time, stays in .tools\
if not exist ".tools" mkdir ".tools"

set "FFURL=https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
set "FFZIP=%CD%\.tools\ffmpeg.zip"

where curl >nul 2>&1
if errorlevel 1 goto :ffmpeg_ps_dl
curl -L --fail --silent --show-error -o "!FFZIP!" "!FFURL!"
if errorlevel 1 goto :ffmpeg_fail
goto :ffmpeg_extract

:ffmpeg_ps_dl
echo       . curl not found, using PowerShell
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { Invoke-WebRequest -Uri '!FFURL!' -OutFile '!FFZIP!' -UseBasicParsing } catch { exit 1 }"
if errorlevel 1 goto :ffmpeg_fail

:ffmpeg_extract
echo       . extracting
powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -LiteralPath '!FFZIP!' -DestinationPath '.tools' -Force"
if errorlevel 1 goto :ffmpeg_fail

for /d %%d in (".tools\ffmpeg-master-*") do (
    if exist ".tools\ffmpeg" rmdir /s /q ".tools\ffmpeg" >nul 2>&1
    move "%%d" ".tools\ffmpeg" >nul 2>&1
)
if exist "!FFZIP!" del /q "!FFZIP!" >nul 2>&1

if not exist ".tools\ffmpeg\bin\ffmpeg.exe" goto :ffmpeg_fail

set "PATH=%CD%\.tools\ffmpeg\bin;!PATH!"
echo       v ffmpeg ready in .tools\ffmpeg
goto :ffmpeg_done

:ffmpeg_fail
echo       ! ffmpeg unavailable
echo         install manually: winget install Gyan.FFmpeg
echo         continuing - single-format downloads will still work

:ffmpeg_done

REM ---- 5. launch ------------------------------------------------------------
echo.
echo   [5] Starting GrabTube
echo.
echo       -^> http://%BIND_HOST%:%PORT%
echo.
echo       press Ctrl+C to stop
echo.

if "%NO_BROWSER%"=="0" (
    start "" /min cmd /c "timeout /t 2 /nobreak >nul && start http://%BIND_HOST%:%PORT%"
)

".venv\Scripts\python.exe" app.py --host %BIND_HOST% --port %PORT% !PASSTHRU!

echo.
echo   GrabTube has stopped.
echo.
pause
exit /b 0

REM ---- error paths ----------------------------------------------------------
:no_python
echo       x Python not found.
echo.
echo       Install it one of these ways, then re-run run.bat:
echo.
where winget >nul 2>&1
if errorlevel 1 goto :no_python_manual

echo         winget install Python.Python.3.12
echo.
set /p "ANS=      Or type Y and I'll run that now: "
if /I "!ANS!"=="Y" (
    winget install --id Python.Python.3.12 -e --source winget --accept-package-agreements --accept-source-agreements
    echo.
    echo       Python installed. Close this window and run run.bat again.
    echo.
    pause
    exit /b 0
)

:no_python_manual
echo         Download from https://www.python.org/downloads/
echo         Tick "Add Python to PATH" during install.
echo.
pause
exit /b 1

:bad_python
echo       x Python 3.10 or newer required
echo         found:
%PY% --version
echo.
pause
exit /b 1

:pip_failed
echo.
echo       x pip install failed - see output above
echo.
pause
exit /b 1