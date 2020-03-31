@echo off
call osvcenv.cmd
"%OSVCPYTHONEXEC%" "%OSVCROOT%\opensvc\om.py" %*
