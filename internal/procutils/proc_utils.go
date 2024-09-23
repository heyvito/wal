package procutils

import (
	"bytes"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

var procStateReg = regexp.MustCompile(`^\s*(\S+)\s*(\S+)`)

// GetPIDState obtains the `stat` flags from the system process table for a
// given PID. This is a rather expensive operation, and should be used with
// caution.
func GetPIDState(pid int) (ProcessState, error) {
	stdout := new(bytes.Buffer)
	cmd := exec.Command("ps", "ax", "-o", "pid,stat")
	cmd.Stdout = stdout
	cmd.Stderr = nil
	cmd.Stdin = nil
	err := cmd.Run()
	if err != nil {
		return 0, fmt.Errorf("failed executing process: %w", err)
	}

	return findProcStateFromPSTable(stdout.String(), pid)
}

func findProcStateFromPSTable(table string, pid int) (ProcessState, error) {
	data := strings.Split(table, "\n")
	for _, v := range data {
		line := strings.TrimSpace(v)
		if len(line) == 0 {
			continue
		}
		components := procStateReg.FindStringSubmatch(line)
		if len(components) != 3 {
			continue
		}
		procID, err := strconv.Atoi(components[1])
		if err != nil {
			continue
		}
		if procID != pid {
			continue
		}

		var state ProcessState
		for _, flag := range components[2] {
			value, ok := stateStringToState[flag]
			if !ok {
				continue
			}
			if state == 0 {
				state = value
			} else {
				state |= value
			}
		}
		return state, nil
	}

	return 0, fmt.Errorf("process not found on process table")
}

type ProcessState uint32

const (
	StateUninterruptibleSleep ProcessState = 1 << iota
	StateRunning
	StateInterruptibleSleep
	StateStopped
	StateTracingStop
	StateDead
	StateDefunct
	StateWakeKill
	StateWaking
	StateParked
	StateIdle

	// Below are possible BSD variants

	StateHighPriority
	StateLowPriority
	StatePagesLocked
	StateSessionLeader
	StateMultiThreaded
	StateForeground
)

var stateStringToState = map[rune]ProcessState{
	'D': StateUninterruptibleSleep,
	'R': StateRunning,
	'S': StateInterruptibleSleep,
	'T': StateStopped,
	't': StateTracingStop,
	'X': StateDead,
	'x': StateDead,
	'K': StateWakeKill,
	'W': StateWaking,
	'P': StateParked,
	'I': StateIdle,
	'Z': StateDefunct,
	'<': StateHighPriority,
	'N': StateLowPriority,
	'L': StatePagesLocked,
	's': StateSessionLeader,
	'l': StateMultiThreaded,
	'+': StateForeground,
}
