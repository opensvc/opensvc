!define PRODUCT_TITLE "${PRJTITLE} Bundle"
!define PRODUCT_NAME "${PRJNAME}"
!define PRODUCT_MANUFACTURER "${PRJMANUFACTURER}"
!define PRODUCT_VERSION "${PRJVERSION}"
!define PRODUCT_PUBLISHER "${PRJBUILDER}"
!define PRODUCT_WEB_SITE "${PRJWEBSITE}"

; MUI 2 compatible ------
!include "MUI2.nsh"

;!include Sections.nsh

; MUI Settings
!define MUI_ABORTWARNING
!define MUI_ICON "${OSVCNSIS}\opensvc.ico"

!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH
!insertmacro MUI_LANGUAGE "English"

# Variables
Var VCRedistInstalled
Var OSVCInstalled

Name "${PRODUCT_TITLE} ${PRODUCT_VERSION}"
OutFile "${PRODUCT_NAME}.${PRODUCT_VERSION}.exe"
InstallDir "$PROGRAMFILES\${PRODUCT_NAME}"

VIProductVersion ${PRODUCT_VERSION}.0.0
VIAddVersionKey ProductName "${PRODUCT_TITLE}"
VIAddVersionKey Comments "A build of the PortableApps.com Launcher for ${PRODUCT_TITLE}, allowing it to be run from a removable drive.  For additional details, visit PortableApps.com"
VIAddVersionKey CompanyName "${PRODUCT_MANUFACTURER}"
VIAddVersionKey LegalCopyright "${PRODUCT_MANUFACTURER}"
VIAddVersionKey FileDescription "${PRODUCT_TITLE}"
VIAddVersionKey FileVersion ${PRODUCT_VERSION}
VIAddVersionKey ProductVersion ${PRODUCT_VERSION}
VIAddVersionKey InternalName "${PRODUCT_TITLE}"
VIAddVersionKey LegalTrademarks "${PRODUCT_MANUFACTURER} is a French registered company."
VIAddVersionKey OriginalFilename "${PRODUCT_NAME}.${PRODUCT_VERSION}.exe"

ShowInstDetails show

Function .onInit  
  ;Check earlier installation
  Call CheckOSVC
        ${If} $OSVCInstalled == 1
        DetailPrint "${PRODUCT_TITLE} is already installed. Going to version ${PRODUCT_VERSION}"
        ${EndIf}
FunctionEnd

Section "MainSection" SEC01
  SetOutPath "$INSTDIR"
  SetOverwrite ifnewer
  File /r "tmp"
SectionEnd

Section "Visual C++ 2008 Runtime" SEC02
  Call CheckVCRedist
        ${If} $VCRedistInstalled == 1
        DetailPrint "VC Runtime is already installed."
        ${Else}
           SetOutPath $INSTDIR
           SetOverwrite on
           #run runtime installation tool
           DetailPrint "Installing Microsoft Visual C++ 2008 SP1 Redistributable Package (x86)"
           ExecWait '"$INSTDIR\tmp\vcredist_x86.exe" /q /norestart'
           DetailPrint "Done"
       ${EndIf}
SectionEnd

Section "${PRODUCT_NAME} msi" SEC03
  ExecWait 'msiexec /i "$INSTDIR\tmp\${PRODUCT_NAME}.${PRODUCT_VERSION}.msi" /quiet INSTALLFOLDER="$INSTDIR"'
SectionEnd

;-------------------------------
; Test if Visual Studio Redistributables 2008 SP1 installed
; Returns -1 if there is no VC redistributables installed
Function CheckVCRedist
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
FunctionEnd

;-------------------------------
; Test if OpenSVC Agent is already installed
; Returns -1 if there is no osvc software installed
Function CheckOSVC
   StrCpy $OSVCInstalled "1"

   ClearErrors
   # SP 1
   ReadRegDword $R0 HKLM "SOFTWARE\${PRODUCT_NAME}" "installed"
   IfErrors 0 OSVCInstalled

   # Runtime not found
   StrCpy $OSVCInstalled 0

OSVCInstalled:
FunctionEnd
