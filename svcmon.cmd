@echo off
call osvcenv.cmd
"%OSVCPYTHONEXEC%" "%OSVCROOT%\opensvc\svcmon.py" %*
