# Prerequisites
# wix tools
# nsis
# opensvc pulled from git server
# kits folder filled in (vcredist and python) 
#

!if !defined(OSVCMAKEVERSION) | !defined(OSVCMAKERELEASE)
MSG = ^
You will need to run osvcvars.bat, first, to setup^
the environment. OSVCMAKEVERSION and OSVCMAKERELEASE have to be defined.
!error $(MSG)
!endif

# App Info
PRJMANUFACTURER=OpenSVC
PRJBUILDER=Arnaud Veron
PRJNAME=OpenSVC
PRJFWRULE1NAME=OpenSVC Listener
PRJFWRULE2NAME=OpenSVC API WebApp Listener
PRJTITLE=OpenSVC Agent Distribution
PRJWEBSITE=https://www.opensvc.com/
PRJVERSION=$(OSVCMAKEVERSION)
PRJRELEASE=$(OSVCMAKERELEASE)

# Windows Service
PRJSVCNAME=OsvcAgent
PRJSVCDISPLAYNAME=OpenSVC Agent
PRJSVCDESC=Orchestration, HA, inventoring, monitoring, config mgmt
PRJSVCPYEXECPATH=python\lib\site-packages\win32
PRJSVCPYEXECBIN=PythonService.exe

# Tools
WIXTOOLS=$(WIX)\bin                     				# Wix Install Directory
NSISTOOLS=C:\Program Files (x86)\NSIS   				# NSIS Install Directory
OSVCROOT=C:\Program Files\$(PRJNAME) 				# OpenSVC Software Dev Directory
OSVCBUILD=$(OSVCROOT)\bin\pkg\winbuilder	# OpenSVC Builder Directory


# You should not need to change variables below
OSVCWIX=$(OSVCBUILD)\wix
OSVCNSIS=$(OSVCBUILD)\nsis
OSVCPKGROOT=C:\windows\temp\osvcpkgroot
EXCLUDEFILE=$(WXSDIR)\ExcludeList.txt
TEMPO=C:\windows\temp\ExcludeList.txt

OUTDIR=$(OSVCBUILD)\wix
WXSDIR=$(OSVCBUILD)\wxs
VCREDISTX64=$(OSVCBUILD)\kits\vcredist
PYTHON=$(OSVCBUILD)\kits\winpython\python

XSLFILE=$(WXSDIR)\opensvc.transform.xsl

all: build

build: copy version clean heat candle light movensis nsisexe

copy:
        @echo =^> Erasing old build directories
		@if exist "$(OSVCPKGROOT)" rmdir "$(OSVCPKGROOT)" /s /q
        @echo =^> Preparing build directories
		@if exist "$(OSVCNSIS)\tmp" rmdir "$(OSVCNSIS)\tmp" /s /q
		@if not exist "$(OSVCWIX)" mkdir "$(OSVCWIX)"
		@mkdir "$(OSVCNSIS)\tmp"
        @mkdir "$(OSVCPKGROOT)"
		@mkdir "$(OSVCPKGROOT)\etc"
		@mkdir "$(OSVCPKGROOT)\var"
		@mkdir "$(OSVCPKGROOT)\var\cache"
		@mkdir "$(OSVCPKGROOT)\var\lock"
		@mkdir "$(OSVCPKGROOT)\log"
		@mkdir "$(OSVCPKGROOT)\tmp"
		@mkdir "$(OSVCPKGROOT)\python"
		@echo =^> Copying Exclude file to temp location
		@copy  "$(EXCLUDEFILE)" $(TEMPO)
		@echo =^> Copying OpenSVC sources to build directory
		@xcopy "$(OSVCROOT)" "$(OSVCPKGROOT)" /s /i /q /exclude:$(TEMPO)
		@echo =^> Copying Microsoft Visual C++ 2015 Redistributable installer to build directory
        @xcopy "$(VCREDISTX64)" "$(OSVCNSIS)\tmp" /s /i /q
		@echo =^> Copying Python distribution to build directory
		@xcopy "$(PYTHON)" "$(OSVCPKGROOT)\python" /s /i /q
		@del /s /q "$(OSVCPKGROOT)\python\*.pyc" 2>nul

version:
	    @echo =^> Generating version.py file in "$(OSVCPKGROOT)\opensvc\utilities\version\"
	    @if not exist "$(OSVCPKGROOT)" mkdir "$(OSVCPKGROOT)"
	    @if not exist "$(OSVCPKGROOT)\opensvc" mkdir "$(OSVCPKGROOT)\opensvc"
	    @if not exist "$(OSVCPKGROOT)\opensvc\utilities" mkdir "$(OSVCPKGROOT)\opensvc\utilities"
	    @if not exist "$(OSVCPKGROOT)\opensvc\utilities\version" mkdir "$(OSVCPKGROOT)\opensvc\utilities\version"
           echo version = "$(OSVCMAKEVERSION)-$(OSVCMAKERELEASE)" > "$(OSVCPKGROOT)\opensvc\utilities\version\version.py" 


clean:
	    @echo =^> Removing old objects files 
		@del /s /q "$(OSVCWIX)\*.wixobj" 2>nul
	    @del /s /q "$(OSVCWIX)\*.wixpdb" 2>nul
        @del /s /q "$(OSVCWIX)\*.msi" 2>nul
        @del /s /q "$(OSVCPKGROOT)\tmp\exc*" 2>nul
        @del /s /q "$(OSVCPKGROOT)\log\*.log" 2>nul
		
heat:
		@echo =^> Harvesting build directory files
		"$(WIXTOOLS)\heat.exe" dir "$(OSVCPKGROOT)" -t "$(XSLFILE)" -nologo -cg OpenSVCFiles -gg -ke -scom -sreg -sfrag -srd -dr INSTALLFOLDER -var var.OSVCPKGROOT -out "$(WXSDIR)\$(PRJNAME)files.wxs"
# -var var.OSVCPKGROOT  is the folder where we build the filelist to include in the msi file

candle:
		@echo =^> Compiling xml documents to object files
		"$(WIXTOOLS)\candle.exe" -arch x64 -ext WiXFirewallExtension -dOSVCPKGROOT="$(OSVCPKGROOT)" -dPRJSVCNAME="$(PRJSVCNAME)" -dPRJSVCDESC="$(PRJSVCDESC)" -dPRJSVCDISPLAYNAME="$(PRJSVCDISPLAYNAME)" -dPRJFWRULE1NAME="$(PRJFWRULE1NAME)" -dPRJFWRULE2NAME="$(PRJFWRULE2NAME)" -dPRJSVCPYEXECPATH="$(PRJSVCPYEXECPATH)" -dPRJSVCPYEXECBIN="$(PRJSVCPYEXECBIN)" -dPRJMANUFACTURER="$(PRJMANUFACTURER)" -dPRJNAME="$(PRJNAME)" -dPRJTITLE="$(PRJTITLE)" -dPRJVERSION="$(PRJVERSION)" -dPRJRELEASE="$(PRJRELEASE)"  "$(WXSDIR)\$(PRJNAME)product.wxs" "$(WXSDIR)\$(PRJNAME)files.wxs" -out "$(OUTDIR)\\"

light:
		@echo =^> Linking objects files to generate final msi
		"$(WIXTOOLS)\light.exe" -ext WixUIExtension -ext WixFirewallExtension "$(OUTDIR)\$(PRJNAME)files.wixobj" "$(OUTDIR)\$(PRJNAME)product.wixobj" -out "$(OUTDIR)\$(PRJNAME).$(PRJVERSION).$(PRJRELEASE).msi"

lightv:
		@echo =^> Linking objects files to generate final msi (VERBOSE LOGGING)
		"$(WIXTOOLS)\light.exe" -v -ext WixUIExtension -ext WixFirewallExtension "$(OUTDIR)\$(PRJNAME)files.wixobj" "$(OUTDIR)\$(PRJNAME)product.wixobj" -out "$(OUTDIR)\$(PRJNAME).$(PRJVERSION).$(PRJRELEASE).msi"

movensis:
        @echo =^> Copying msi installer to nsis build directory
		@copy /V /Y "$(OSVCWIX)\$(PRJNAME).$(PRJVERSION).$(PRJRELEASE).msi" "$(OSVCNSIS)\tmp\"

nsisexe:
        @echo =^> Building nsis exe installer
		"$(NSISTOOLS)\makensis.exe" /DPRJMANUFACTURER="$(PRJMANUFACTURER)" /DPRJNAME="$(PRJNAME)" /DPRJTITLE="$(PRJTITLE)" /DPRJVERSION="$(PRJVERSION)" /DPRJRELEASE="$(PRJRELEASE)" /DPRJBUILDER="$(PRJBUILDER)" /DPRJWEBSITE="$(PRJWEBSITE)" /DOSVCNSIS="$(OSVCNSIS)" "$(OSVCNSIS)\$(PRJNAME).nsi"
		@echo =^> 
		@echo =^> OpenSVC installer is located here : $(OSVCNSIS)
		@echo =^> 
		@start explorer.exe /e,"$(OSVCNSIS)\"
		
