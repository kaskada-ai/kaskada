package property

type PrepareJobState string

const (
	PrepareJobStateUnspecified PrepareJobState = "UNSPECIFIED"
	PrepareJobStateSubmitted   PrepareJobState = "SUBMITTED"
	PrepareJobStateStarted     PrepareJobState = "STARTED"
	PrepareJobStateFinished    PrepareJobState = "FINISHED"
	PrepareJobStateErrored     PrepareJobState = "ERRORED"
)

// Values provides list valid values for Enum.
func (PrepareJobState) Values() (kinds []string) {
	states := []PrepareJobState{
		PrepareJobStateUnspecified,
		PrepareJobStateSubmitted,
		PrepareJobStateStarted,
		PrepareJobStateFinished,
		PrepareJobStateErrored,
	}

	for _, s := range states {
		kinds = append(kinds, string(s))
	}
	return
}
