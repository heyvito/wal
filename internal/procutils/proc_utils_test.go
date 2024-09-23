package procutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindProcStateFromPSTable(t *testing.T) {
	table := "PID   STAT\n    1 S\n    2 SW\n    3 SW\n    4 IW<\n 3454 Z\n"

	expectedStates := map[int]ProcessState{
		1:    StateInterruptibleSleep,
		2:    StateInterruptibleSleep | StateWaking,
		3:    StateInterruptibleSleep | StateWaking,
		4:    StateIdle | StateWaking | StateHighPriority,
		3454: StateDefunct,
	}

	for pid, expectedState := range expectedStates {
		state, err := findProcStateFromPSTable(table, pid)
		require.NoError(t, err)
		assert.Equalf(t, expectedState, state, "Expected PID %d to have state %s, found %s instead", pid, expectedState, state)
	}
}
