
set GIT="C:\Program Files (x86)\Git\bin\"
set BCKPATH=%PATH%
set PATH=%PATH%;%GIT%
set OSVCROOT="C:\Program Files\OpenSVC"

pushd %OSVCROOT%

for /F "tokens=*" %%i in ('git.exe describe --tags ^| gawk -F^'-^' ^'{ print $1 }^'') do set OSVCPRJVERSION=%%i

popd &rem

set PATH=%BCKPATH%