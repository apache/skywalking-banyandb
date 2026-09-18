========================================================================
Canopy related licenses
========================================================================

The following components are used in Canopy. See project link for details.
The text of each license is also included at licenses/license-[project].txt.
 
{{ range .Groups }}
========================================================================
{{.LicenseID}} licenses
========================================================================
{{range .Deps}}
    {{.Name}} {{.Version}} {{.LicenseID}}
{{- end }}
{{ end }}
