//go:build !windows

package cmd

// windowsSetupFlow is a no-op stub on non-Windows platforms.
func windowsSetupFlow() error {
	return printUsage()
}
