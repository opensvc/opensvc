@echo off

set OSVCROOT=%~1
if %OSVCROOT:~-1%==\ set OSVCROOT=%OSVCROOT:~0,-1%
set OSVCPYTHONROOT=%OSVCROOT%\python
set OSVCPYTHONEXEC=%OSVCPYTHONROOT%\python.exe
set PYTHONPATH=%OSVCROOT%\lib
call "%OSVCROOT%\inpath.cmd" OSVCROOT
call "%OSVCROOT%\inpath.cmd" OSVCPYTHONROOT
"%OSVCPYTHONEXEC%" "%OSVCROOT%\bin\postinstall" 
if errorlevel 1 (
   echo Failure Reason Given is %errorlevel%
   pause
    exit /b %errorlevel%
)
exit /b 0
