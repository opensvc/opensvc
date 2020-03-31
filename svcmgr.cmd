@echo off
call osvcenv.cmd
"%OSVCPYTHONEXEC%" "%OSVCROOT%\opensvc\svcmgr.py" %*
