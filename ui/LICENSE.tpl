========================================================================
UI related licenses
========================================================================

The following components are used in UI.See project link for details.
The text of each license is also included at licenses/ui-licenses/LICENSE-[project].txt.
 
{{ range .Groups }}
========================================================================
{{.LicenseID}} licenses
========================================================================
{{range .Deps}}
    {{.Name}} {{.Version}} {{.LicenseID}}
{{- end }}
{{ end }}