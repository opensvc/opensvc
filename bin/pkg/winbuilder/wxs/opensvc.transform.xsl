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
  <!-- Search Python Service Executable -->
  <xsl:key name="pythonservice" match="wix:Component[contains(wix:File/@Source, 'pythonservice.exe')]" use="@Id" />
  
  <!-- Search postinstall script -->
  <xsl:key name="vIdToReplace" match="wix:File[contains(@Source, '\bin\postinstall.cmd')]" use="@Id" />

  <!-- Remove file component -->
  <xsl:template match="wix:Component[key('pythonservice', @Id)]" />

  <!-- Remove componentsrefs referencing components -->
  <xsl:template match="wix:ComponentRef[key('pythonservice', @Id)]" />  

  <xsl:template match="node()[key('vIdToReplace', @Id)]">
    <xsl:copy>
      <xsl:attribute name="Id">OSVC_POSTINSTALL_CMD</xsl:attribute>
      <xsl:copy-of select="@*[name()!='Id']"/>
      <xsl:apply-templates />
    </xsl:copy>
  </xsl:template>

  <!-- ### Adding the Win64-attribute to all Components -->
  <xsl:template match="wix:Component">
    <xsl:copy>
      <xsl:apply-templates select="@*" />
        <!-- Adding the Win64-attribute as we have a x64 application -->
        <xsl:attribute name="Win64">yes</xsl:attribute>

        <!-- Now take the rest of the inner tag -->
        <xsl:apply-templates select="node()" />
    </xsl:copy>
  </xsl:template>
  
  
  </xsl:stylesheet>
