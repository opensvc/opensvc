<?xml version="1.0" ?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:wix="http://schemas.microsoft.com/wix/2006/wi">

  <!-- Copy all attributes and elements to the output. -->
  <xsl:template match="@*|*">
    <xsl:copy>
      <xsl:apply-templates select="@*" />
      <xsl:apply-templates select="*" />
    </xsl:copy>
  </xsl:template>

  <xsl:output method="xml" indent="yes" />
  <!-- PythonService.exe Search Python Service Executable -->
  <xsl:key name="pythonservice" match="wix:Component[contains(wix:File/@Source, 'PythonService.exe')]" use="@Id" />
  
  <!-- PythonService.exe Search Python Service Executable -->
  <xsl:key name="vIdToReplace" match="wix:File[contains(@Source, '\bin\postinstall.cmd')]" use="@Id" />

  <!-- PythonService.exe Remove file component -->
  <xsl:template match="wix:Component[key('pythonservice', @Id)]" />

  <!-- PythonService.exe  Remove componentsrefs referencing components -->
  <xsl:template match="wix:ComponentRef[key('pythonservice', @Id)]" />  

  <xsl:template match="node()[key('vIdToReplace', @Id)]">
    <xsl:copy>
      <xsl:attribute name="Id">OSVC_POSTINSTALL_CMD</xsl:attribute>
      <xsl:copy-of select="@*[name()!='Id']"/>
      <xsl:apply-templates />
    </xsl:copy>
  </xsl:template>

  
  
  </xsl:stylesheet>
