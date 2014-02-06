!define PRODUCT_TITLE "${PRJTITLE} Bundle"
!define PRODUCT_NAME "${PRJNAME}"
!define PRODUCT_MANUFACTURER "${PRJMANUFACTURER}"
!define PRODUCT_VERSION "${PRJVERSION}"
!define PRODUCT_RELEASE "${PRJRELEASE}"
!define PRODUCT_PUBLISHER "${PRJBUILDER}"
!define PRODUCT_WEB_SITE "${PRJWEBSITE}"

; MUI 2 compatible ------
!include "MUI2.nsh"

# Variables
Var /GLOBAL VCRedistInstalled
Var /GLOBAL OSVCInstalled
Var /GLOBAL NSISLogFile 

;!include Sections.nsh

; MUI Settings
!define MUI_ABORTWARNING
!define MUI_ICON "${OSVCNSIS}\opensvc.ico"

!insertmacro MUI_PAGE_WELCOME
!define MUI_PAGE_CUSTOMFUNCTION_PRE DoWeNeedPageDirectory
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH
!insertmacro MUI_LANGUAGE "English"

!include "textlog.nsh"

Name "${PRODUCT_TITLE} ${PRODUCT_VERSION}.${PRODUCT_RELEASE}"
OutFile "${PRODUCT_NAME}.${PRODUCT_VERSION}.${PRODUCT_RELEASE}.exe"
InstallDir "$PROGRAMFILES\${PRODUCT_NAME}"

# 3 digits sections expected in PRODUCT_VERSION (example 1.5.1401230658)
# VIProductVersion is restricted...
# Product version must have a major version less than 256, a minor version less than 256, and a build version less than 65536.
VIProductVersion ${PRODUCT_VERSION}.${PRODUCT_RELEASE}.0

VIAddVersionKey ProductName "${PRODUCT_TITLE}"
VIAddVersionKey Comments "A build of the ${PRODUCT_TITLE} product.  For additional details, visit OpenSVC.com"
VIAddVersionKey CompanyName "${PRODUCT_MANUFACTURER}"
VIAddVersionKey LegalCopyright "${PRODUCT_MANUFACTURER}"
VIAddVersionKey FileDescription "${PRODUCT_TITLE}"
VIAddVersionKey FileVersion "${PRODUCT_VERSION}.${PRODUCT_RELEASE}"
VIAddVersionKey ProductVersion "${PRODUCT_VERSION}.${PRODUCT_RELEASE}"
VIAddVersionKey InternalName "${PRODUCT_TITLE}"
VIAddVersionKey LegalTrademarks "${PRODUCT_MANUFACTURER} is a French registered company."
VIAddVersionKey OriginalFilename "${PRODUCT_NAME}.${PRODUCT_VERSION}.${PRODUCT_RELEASE}.exe"

ShowInstDetails show

Function .onInit
  StrCpy $NSISLogFile "$TEMP\${PRODUCT_NAME}.${PRODUCT_VERSION}.${PRODUCT_RELEASE}.nsis.install.log"
  ${LogSetFileName} $NSISLogFile
  ${LogSetOn}
  ${LogText} "[onInit] Begin"
  ;Check earlier installation
  Call CheckOSVC
  ${LogText} "[onInit] End"
FunctionEnd

Section "MainSection" SEC01
  ${LogText} "[SEC01] Begin"
  ${LogText} "[SEC01] Will set outpath to <$INSTDIR>"
  SetOutPath "$INSTDIR"
  ${If} $OSVCInstalled == 1
		${LogText} "[SEC01] Previous OpenSVC installation detected"
                ${LogText} "[SEC01] Will read Installation directory from registry"
                ReadRegStr $R1 HKLM "SOFTWARE\${PRODUCT_NAME}" "path"
                ${LogText} "[SEC01] Read <$R1> Installation directory from registry"
        ; not supposed to have read error here
		${LogText} "[SEC01] Will set outpath to <$R1>"
                SetOutPath "$R1"
		DetailPrint "${PRODUCT_TITLE} is already installed. Going to version ${PRODUCT_VERSION}"
		DetailPrint "${PRODUCT_TITLE} is installed in folder $OUTDIR"
  ${Else}
                ${LogText} "[SEC01] First OpenSVC installation detected"
  ${EndIf}
  SetOverwrite ifnewer
  ${LogText} "[SEC01] Will uncompress tmp folder"
  File /r "tmp"
  ${LogText} "[SEC01] End"
SectionEnd

Section "Visual C++ 2008 Runtime" SEC02
  ${LogText} "[SEC02] Begin"
  ${LogText} "[SEC02] Will check for VCRedist already installed"
  Call CheckVCRedist
        ${If} $VCRedistInstalled == 1
           ${LogText} "[SEC02] VCRedist is already installed"
           DetailPrint "VC Runtime is already installed."
        ${Else}
           ${LogText} "[SEC02] VCRedist is not installed"
           SetOverwrite on
           #run runtime installation tool
           DetailPrint "Installing Microsoft Visual C++ 2008 SP1 Redistributable Package (x86)"
           ${LogText} "[SEC02] Will trigger vcredist_x86.exe installation"
           ExecWait '"$INSTDIR\tmp\vcredist_x86.exe" /q /norestart' $0
           Call CheckReturnCode
           ${LogText} "[SEC02] vcredist_x86.exe installation done"
           DetailPrint "Done"
       ${EndIf}
  ${LogText} "[SEC02] End"
SectionEnd

Section "${PRODUCT_NAME} msi" SEC03
  ${LogText} "[SEC03] Begin"
  ${LogText} "[SEC03] Will start OpenSVC msi installation"
  ExecWait 'msiexec /i "$OUTDIR\tmp\${PRODUCT_NAME}.${PRODUCT_VERSION}.${PRODUCT_RELEASE}.msi" /l*v "$OUTDIR\tmp\${PRODUCT_NAME}.${PRODUCT_VERSION}.${PRODUCT_RELEASE}.msiexec.log" /quiet INSTALLFOLDER="$OUTDIR"' $0
  Call CheckReturnCode
  ${LogText} "[SEC03] OpenSVC msi installation done"
  ${LogText} "[SEC03] End"
SectionEnd

Section "CleanUp" SEC04
    ${LogText} "[SEC04] Begin"
    ${LogText} "Closing log file"
    ${LogText} "[SEC04] End"
    ${LogSetOff}
    CopyFiles /SILENT "$NSISLogFile" "$OUTDIR\tmp\"
SectionEnd

;-------------------------------
; Test if Visual Studio Redistributables 2008 SP1 installed
; Returns -1 if there is no VC redistributables installed
Function CheckVCRedist
   ${LogText} "[CheckVCRedist] Begin"
   StrCpy $VCRedistInstalled "1"

   ClearErrors
   # SP 1
   ReadRegDword $R0 HKLM "SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{9A25302D-30C0-39D9-BD6F-21E6EC160475}" "Version"
   IfErrors 0 VSRedistInstalled

   # SP 1 + ATL Security Update
   ClearErrors
   ReadRegDword $R0 HKLM "SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{1F1C2DFC-2D24-3E06-BCB8-725134ADF989}" "Version"
   IfErrors 0 VSRedistInstalled

   # Runtime not found
   StrCpy $VCRedistInstalled 0

VSRedistInstalled:
  ${LogText} "[CheckVCRedist] End"
FunctionEnd

;-------------------------------
; Test if OpenSVC Agent is already installed
; Returns -1 if there is no osvc software installed
Function CheckOSVC
   ${LogText} "[CheckOSVC] Begin"
   StrCpy $OSVCInstalled "1"

   ClearErrors
   ReadRegDword $R0 HKLM "SOFTWARE\${PRODUCT_NAME}" "installed"
   IfErrors 0 OSVCInstalled

   StrCpy $OSVCInstalled 0

   OSVCInstalled:
   ${LogText} "[CheckOSVC] End"
FunctionEnd

;-------------------------------
; Test if Directory Page is needed, yes if first install, no if upgrade
Function DoWeNeedPageDirectory
  ${LogText} "[DoWeNeedPageDirectory] Begin"
  ${If} $OSVCInstalled == 1
        ${LogText} "[DoWeNeedPageDirectory] OpenSVC already installed. No need for folder window."
        Abort
  ${EndIf}
  ${LogText} "[DoWeNeedPageDirectory] End"
FunctionEnd

;-------------------------------
; Test what happens with msiexec
Function CheckReturnCode
  ${LogText} "[CheckReturnCode] Begin"
  DetailPrint "Installer return code was <$0>"  
  ${If} $0 != 0 
    ${LogText} "[CheckReturnCode] Return code is <$0>. Aborting."
    Abort "There was a problem installing the application."
  ${Else}
    ${LogText} "[CheckReturnCode] Return code is <$0>. OK."
  ${EndIf}
  ${LogText} "[CheckReturnCode] End"
FunctionEnd