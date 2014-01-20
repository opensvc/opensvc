# Prerequisites
# wix tools
# nsis
# opensvc pulled from git server
# kits folder filled in (vcredist and python) 
#

!if !defined(OSVCPRJVERSION)
MSG = ^
You will need to run osvcvars.bat, first, to setup^
the environment. OSVCPRJVERSION have to be defined.
!error $(MSG)
!endif

# App Info
PRJMANUFACTURER=OpenSVC SARL
PRJBUILDER=Arnaud Veron
PRJNAME=OpenSVC
PRJFWRULE1NAME=OpenSVC Listener
PRJTITLE=OpenSVC Agent Distribution
PRJWEBSITE=http://www.opensvc.com/
PRJVERSION=$(OSVCPRJVERSION)

# Windows Service
PRJSVCNAME=OsvcSched
PRJSVCDISPLAYNAME=OpenSVC job scheduler
PRJSVCDESC=Schedule the OpenSVC jobs
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
VCREDISTX86=$(OSVCBUILD)\kits\vcredist
PYTHON=$(OSVCBUILD)\kits\winpython\python

XSLFILE=$(WXSDIR)\opensvc.transform.xsl

all: build

build: copy clean heat candle light movensis nsisexe

copy:
        @if exist "$(OSVCPKGROOT)" rmdir "$(OSVCPKGROOT)" /s /q
        @rmdir "$(OSVCNSIS)\tmp" /s /q
		@mkdir "$(OSVCNSIS)\tmp"
        @mkdir "$(OSVCPKGROOT)"
		@mkdir "$(OSVCPKGROOT)\etc"
		@mkdir "$(OSVCPKGROOT)\var\cache"
		@mkdir "$(OSVCPKGROOT)\var\lock"
		@mkdir "$(OSVCPKGROOT)\log"
		@mkdir "$(OSVCPKGROOT)\tmp"
		@mkdir "$(OSVCPKGROOT)\python"
		@copy  "$(EXCLUDEFILE)" $(TEMPO)
		@xcopy "$(OSVCROOT)" "$(OSVCPKGROOT)" /s /i /exclude:$(TEMPO)	
        @xcopy "$(VCREDISTX86)" "$(OSVCNSIS)\tmp" /s /i
		@xcopy "$(PYTHON)" "$(OSVCPKGROOT)\python" /s /i

clean:
	    @del /s /q "$(OSVCWIX)\*.wixobj" 2>nul
	    @del /s /q "$(OSVCWIX)\*.wixpdb" 2>nul
        @del /s /q "$(OSVCWIX)\*.msi" 2>nul
        @del /s /q "$(OSVCPKGROOT)\tmp\exc*" 2>nul
        @del /s /q "$(OSVCPKGROOT)\log\*.log" 2>nul
		
heat:
		"$(WIXTOOLS)\heat.exe" dir "$(OSVCPKGROOT)" -t "$(XSLFILE)" -nologo -cg OpenSVCFiles -gg -ke -scom -sreg -sfrag -srd -dr INSTALLFOLDER -var var.OSVCPKGROOT -out "$(WXSDIR)\$(PRJNAME)files.wxs"
# -var var.OSVCPKGROOT  is the folder where we build the filelist to include in the msi file

candle:
		"$(WIXTOOLS)\candle.exe" -ext WiXFirewallExtension -dOSVCPKGROOT="$(OSVCPKGROOT)" -dPRJSVCNAME="$(PRJSVCNAME)" -dPRJSVCDESC="$(PRJSVCDESC)" -dPRJSVCDISPLAYNAME="$(PRJSVCDISPLAYNAME)" -dPRJFWRULE1NAME="$(PRJFWRULE1NAME)" -dPRJSVCPYEXECPATH="$(PRJSVCPYEXECPATH)" -dPRJSVCPYEXECBIN="$(PRJSVCPYEXECBIN)" -dPRJMANUFACTURER="$(PRJMANUFACTURER)" -dPRJNAME="$(PRJNAME)" -dPRJTITLE="$(PRJTITLE)" -dPRJVERSION="$(PRJVERSION)"  "$(WXSDIR)\$(PRJNAME)product.wxs" "$(WXSDIR)\$(PRJNAME)files.wxs" -out "$(OUTDIR)\\"

light:
		"$(WIXTOOLS)\light.exe" -ext WixUIExtension -ext WixFirewallExtension "$(OUTDIR)\$(PRJNAME)files.wixobj" "$(OUTDIR)\$(PRJNAME)product.wixobj" -out "$(OUTDIR)\$(PRJNAME).$(PRJVERSION).msi"

lightv:
		"$(WIXTOOLS)\light.exe" -v -ext WixUIExtension -ext WixFirewallExtension "$(OUTDIR)\$(PRJNAME)files.wixobj" "$(OUTDIR)\$(PRJNAME)product.wixobj" -out "$(OUTDIR)\$(PRJNAME).$(PRJVERSION).msi"

movensis:
        @copy /V /Y "$(OSVCWIX)\$(PRJNAME).$(PRJVERSION).msi" "$(OSVCNSIS)\tmp\"

nsisexe:
        "$(NSISTOOLS)\makensis.exe" /DPRJMANUFACTURER="$(PRJMANUFACTURER)" /DPRJNAME="$(PRJNAME)" /DPRJTITLE="$(PRJTITLE)" /DPRJVERSION="$(PRJVERSION)" /DPRJBUILDER="$(PRJBUILDER)" /DPRJWEBSITE="$(PRJWEBSITE)" "$(OSVCNSIS)\$(PRJNAME).nsi"
		@echo " "
		@echo "OpenSVC installer is located here : $(OSVCNSIS)\"
