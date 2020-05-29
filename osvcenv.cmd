@echo off
set OSVCROOT=C:\Program Files\opensvc
set OSVCPYTHONROOT=%OSVCROOT%\python
set PYTHONPATH=%OSVCROOT%\opensvc
set OSVCPYTHONEXEC=%OSVCPYTHONROOT%\python.exe
call inpath.cmd OSVCPYTHONROOT
