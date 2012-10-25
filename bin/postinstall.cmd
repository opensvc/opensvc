@echo off
set POSTSCRIPT=%1
for /F "tokens=*" %%i in ('dir /s /l /b /ad /on "%ProgramFiles%\python2*"') do set PYTHONPATH=%%i\python.exe
if exist "%PYTHONPATH%" ("%PYTHONPATH%" %POSTSCRIPT%) else exit 1
exit 0
