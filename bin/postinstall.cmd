@echo off
set OPENSVCPATH=%ProgramFiles%\opensvc
for /F "tokens=*" %%i in ('dir /s /l /b /ad /on "%ProgramFiles%\python2*"') do set PYTHONPATH=%%i\python.exe
if exist "%PYTHONPATH%" ("%PYTHONPATH%" "%OPENSVCPATH%\bin\postinstall")
