

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

if "!MASTER!" equ "0" (
  for /F "tokens=*" %%i in ('git.exe describe --tags ^| cut -d- -f2') do set /A OSVCPRJRELEASE=%%i
  if "!OSVCPRJRELEASE!" equ "!OSVCPRJVERSION!" ( set /A OSVCPRJRELEASE=0 )
  set /A OSVCRELEASE=!OSVCPRJRELEASE!
) else (
  set /A OSVCRELEASE=!OSVCPRJRELEASE!+10000
)

popd &rem

set PATH=%BCKPATH%

echo MASTER=%MASTER%
echo OSVCPRJVERSION=%OSVCPRJVERSION%
echo OSVCPRJHEAD=%OSVCPRJHEAD%
echo OSVCPRJRELEASE=%OSVCPRJRELEASE%

set OSVCMAKEVERSION=%OSVCPRJVERSION%
set OSVCMAKERELEASE=%OSVCRELEASE%
