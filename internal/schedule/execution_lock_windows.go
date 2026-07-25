//go:build windows

package schedule

import (
	"crypto/sha256"
	"fmt"

	"golang.org/x/sys/windows"
)

// tryExecutionLock uses a named mutex because Windows does not provide flock.
// The mutex is released by the OS if the owning process exits.
func tryExecutionLock(path string) (release func(), acquired bool, err error) {
	sum := sha256.Sum256([]byte(path))
	name, err := windows.UTF16PtrFromString(fmt.Sprintf("reasonix-scheduler-%x", sum[:]))
	if err != nil {
		return nil, false, err
	}
	h, err := windows.CreateMutex(nil, false, name)
	if err != nil {
		return nil, false, err
	}
	state, err := windows.WaitForSingleObject(h, 0)
	if err != nil {
		_ = windows.CloseHandle(h)
		return nil, false, err
	}
	if state != windows.WAIT_OBJECT_0 && state != windows.WAIT_ABANDONED {
		_ = windows.CloseHandle(h)
		return nil, false, nil
	}
	return func() {
		_ = windows.ReleaseMutex(h)
		_ = windows.CloseHandle(h)
	}, true, nil
}
