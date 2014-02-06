@echo off
set OSVCROOT=C:\Program Files\opensvc
set OSVCPYTHONROOT=%OSVCROOT%\python
set PYTHONPATH=%OSVCROOT%\lib
set OSVCPYTHONEXEC=%OSVCPYTHONROOT%\python.exe
call inpath.cmd OSVCPYTHONROOT