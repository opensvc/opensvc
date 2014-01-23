

set GIT="C:\Program Files (x86)\Git\bin\"
set BCKPATH=%PATH%
set PATH=%PATH%;%GIT%
set OSVCROOT=C:\Program Files\OpenSVC
set OSVCBUILD=%OSVCROOT%\bin\pkg\winbuilder

pushd %OSVCROOT%

for /F "tokens=*" %%i in ('git.exe describe --tags --abbrev^=0') do set OSVCPRJVERSION=%%i
set MASTER=0
for /F "tokens=*" %%i in ('git.exe branch ^| findstr /B /L /C:"\* master"') do set MASTER=1
for /F "tokens=*" %%i in ('git.exe log -1 --pretty^=format:%%at') do set OSVCPRJHEAD=%%i

set /A OSVCPRJRELEASE=0

if "!MASTER!" equ "0" (
  for /F "tokens=*" %%i in ('git.exe describe --tags ^| cut -d- -f2') do set /A OSVCPRJRELEASE=%%i
  if "!OSVCPRJRELEASE!" equ "!OSVCPRJVERSION!" ( set /A OSVCPRJRELEASE=0 )
  set /A OSVCRELEASE=!OSVCPRJRELEASE!
) else (
  for /F "tokens=*" %%i in ('python.exe -c "import datetime ; print(datetime.datetime.fromtimestamp("%%OSVCPRJHEAD%%").strftime('%%y%%m%%d%%H%%M'))"') do set /A OSVCPRJRELEASE=%%i
  set RELEASEFOUNDININDEX="0"
  for /F "tokens=*" %%i in ('grep.exe %%OSVCPRJRELEASE%% ^"%%OSVCBUILD%%^\%%OSVCPRJVERSION%%^.index^.txt^"') do set RELEASEFOUNDININDEX=1
  if "!RELEASEFOUNDININDEX!" equ "1" (
	for /F "tokens=*" %%i in ('grep.exe %%OSVCPRJRELEASE%% ^"%%OSVCBUILD%%^\%%OSVCPRJVERSION%%^.index^.txt^" ^| gawk -F^'^;^' ^'{ print $1 }^' ') do set /A OSVCINDEX=%%i
	set OSVCLASTINDEX=
  ) else (
	for /F "tokens=*" %%i in ('tail.exe -1 ^"%%OSVCBUILD%%^\%%OSVCPRJVERSION%%^.index^.txt^" ^| gawk -F^'^;^' ^'{ print $1 }^' ') do set /A OSVCLASTINDEX=%%i
	set /A OSVCINDEX=!OSVCLASTINDEX!+1
	echo !OSVCINDEX!;!OSVCPRJRELEASE! >> "%OSVCBUILD%\%OSVCPRJVERSION%.index.txt"
  )
  set /A OSVCRELEASE=!OSVCINDEX!+10000
)

popd &rem

set PATH=%BCKPATH%

echo MASTER=%MASTER%
echo OSVCPRJVERSION=%OSVCPRJVERSION%
echo OSVCPRJHEAD=%OSVCPRJHEAD%
echo OSVCPRJRELEASE=%OSVCPRJRELEASE%
echo RELEASEFOUNDININDEX=%RELEASEFOUNDININDEX%
echo OSVCLASTINDEX=%OSVCLASTINDEX%
echo OSVCINDEX=%OSVCINDEX%
set OSVCMAKEVERSION=%OSVCPRJVERSION%
set OSVCMAKERELEASE=%OSVCRELEASE%
