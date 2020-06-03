@echo off
call osvcenv.cmd
"%OSVCPYTHONEXEC%" "%OSVCROOT%\opensvc\nodemgr.py" %*
