========================================================================
mcp related licenses
========================================================================

The following components are used in MCP.See project link for details.
The text of each license is also included at dist/licenses/mcp-server-licenses/LICENSE-[project].txt.
 
{{ range .Groups }}
========================================================================
{{.LicenseID}} licenses
========================================================================
{{range .Deps}}
    {{.Name}} {{.Version}} {{.LicenseID}}
{{- end }}
{{ end }}