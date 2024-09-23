# flock
Package flock implements a small wrapper around the flock(2) Kernel API in
order to provide advisory locks through the filesystem. It may be important
to notice that flock is an advisory lock, meaning processes are free to
ignore the lock altogether.

For more information and documentation about the exposed API, see
[flock.go](flock.go).