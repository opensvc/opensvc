

set GIT="C:\Program Files (x86)\Git\bin\"
set BCKPATH=%PATH%
set PATH=%PATH%;%GIT%
set OSVCROOT=C:\Program Files\OpenSVC
set OSVCBUILD=%OSVCROOT%\bin\pkg\winbuilder

pushd %OSVCROOT%

for /F "tokens=*" %%i in ('git.exe describe --tags --abbrev^=0') do set OSVCPRJVERSION=%%i
set MASTER=0
for /F "tokens=*" %%i in ('git.exe branch ^| findstr /B /L /C:"\* master"') do set MASTER=1
for /F "tokens=*" %%i in ('git.exe describe --tags ^| cut -d- -f2 ') do set OSVCPRJRELEASE=%%i

set /A OSVCRELEASE=!OSVCPRJRELEASE!

popd &rem

set PATH=%BCKPATH%

echo MASTER=%MASTER%
echo OSVCPRJVERSION=%OSVCPRJVERSION%
echo OSVCPRJRELEASE=%OSVCPRJRELEASE%

set OSVCMAKEVERSION=%OSVCPRJVERSION%
set OSVCMAKERELEASE=%OSVCRELEASE%
