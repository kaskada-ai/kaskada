package schema

import "time"

// Delecare new enum for use above
type DependencyType string

const (
	DependencyType_Unknown DependencyType = "UNKNOWN"
	DependencyType_Table   DependencyType = "TABLE"
	DependencyType_View    DependencyType = "VIEW"
)

// Values provides list valid values for Enum.
func (DependencyType) Values() (kinds []string) {
	for _, s := range []DependencyType{DependencyType_Unknown, DependencyType_Table, DependencyType_View} {
		kinds = append(kinds, string(s))
	}
	return
}

func microsecondNow() time.Time {
	return time.Now().Round(time.Microsecond)
}
