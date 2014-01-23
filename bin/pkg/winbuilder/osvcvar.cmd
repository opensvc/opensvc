
set GIT="C:\Program Files (x86)\Git\bin\"
set BCKPATH=%PATH%
set PATH=%PATH%;%GIT%
set OSVCROOT="C:\Program Files\OpenSVC"

pushd %OSVCROOT%

for /F "tokens=*" %%i in ('git.exe describe --tags --abbrev^=0') do set OSVCPRJVERSION=%%i
set MASTER=0
for /F "tokens=*" %%i in ('git.exe branch ^| findstr /B /L /C:"\* zaster"') do set MASTER=1
for /F "tokens=*" %%i in ('git.exe log -1 --pretty^=format:%%at') do set OSVCPRJHEAD=%%i

if %MASTER% equ 0 (
  for /F "tokens=*" %%i in ('git.exe describe --tags ^| cut -d- -f2') do set OSVCPRJRELEASE=%%i
  if %OSVCPRJRELEASE% equ %OSVCPRJVERSION% ( set OSVCPRJRELEASE=0 )
) else (
  for /F "tokens=*" %%i in ('python.exe -c "import datetime ; print(datetime.datetime.fromtimestamp("%%OSVCPRJHEAD%%").strftime('%%y%%m%%d.%%H%%M'))"') do set OSVCPRJRELEASE=%%i
)

popd &rem

set PATH=%BCKPATH%

echo MASTER=%MASTER%
echo OSVCPRJVERSION=%OSVCPRJVERSION%
echo OSVCPRJHEAD=%OSVCPRJHEAD%
echo OSVCPRJRELEASE=%OSVCPRJRELEASE%