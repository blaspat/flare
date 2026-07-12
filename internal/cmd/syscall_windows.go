package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

var (
	shell32           = syscall.NewLazyDLL("shell32.dll")
	procIsUserAnAdmin = shell32.NewProc("IsUserAnAdmin")
	procShellExecuteW = shell32.NewProc("ShellExecuteW")
)

// windowsIsAdmin returns true if the process is running elevated (as admin).
func windowsIsAdmin() bool {
	ret, _, _ := procIsUserAnAdmin.Call()
	return ret != 0
}

// windowsReElevate re-launches this binary as administrator via UAC prompt.
// The original process should exit after this returns successfully.
func windowsReElevate(arg ...string) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("executable path: %w", err)
	}
	args := strings.Join(arg, " ")
	verbPtr := unsafe.Pointer(syscall.StringToUTF16Ptr("runas"))
	exePtr := unsafe.Pointer(syscall.StringToUTF16Ptr(exe))
	argsPtr := unsafe.Pointer(syscall.StringToUTF16Ptr(args))
	dirPtr := unsafe.Pointer(syscall.StringToUTF16Ptr(filepath.Dir(exe)))

	ret, _, _ := procShellExecuteW.Call(
		0,                // hwnd parent
		uintptr(verbPtr), // verb "runas"
		uintptr(exePtr),  // file
		uintptr(argsPtr), // parameters
		uintptr(dirPtr),  // directory
		1,                // SW_NORMAL
	)
	if ret <= 32 {
		return fmt.Errorf("elevation cancelled or failed (code %d)", ret)
	}
	return nil
}

// windowsSetupFlow runs when Flare is double-clicked on Windows.
// It elevates to admin if needed, installs the service, starts it,
// opens the dashboard, and exits.
func windowsSetupFlow() error {
	// Not admin → re-launch with UAC prompt
	if !windowsIsAdmin() {
		fmt.Print("Flare requires administrator privileges to install as a Windows service.\nRequesting elevation...\n")
		if err := windowsReElevate("start"); err != nil {
			fmt.Printf("Elevation failed: %v\n", err)
			fmt.Println("Please right-click flare.exe and select 'Run as administrator'.")
			pauseBeforeExit()
			return nil
		}
		// Elevation was launched — the new elevated process handles everything
		return nil
	}

	// We're admin — do the full setup
	fmt.Println("Flare — Edge Mesh Server")
	fmt.Println()

	nssmPath, err := windowsEnsureNSSM()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Setup failed: %v\n", err)
		pauseBeforeExit()
		return nil
	}

	windowsStartService()
	fmt.Println()
	fmt.Println("✓ Flare is installed and running!")
	fmt.Println()
	fmt.Println("  Dashboard:  http://localhost:9722")
	fmt.Println("  Logs:       " + filepath.Dir(nssmPath) + "\\logs\\")
	fmt.Println("  Stop:       " + os.Args[0] + " stop")
	fmt.Println()
	fmt.Println("Opening dashboard in your browser...")

	windowsOpenBrowser("http://localhost:9722")
	pauseBeforeExit()
	return nil
}

// pauseBeforeExit waits for a key press so the console window doesn't
// vanish immediately when the user double-clicks.
func pauseBeforeExit() {
	fmt.Print("Press any key to exit...")
	var buf [1]byte
	os.Stdin.Read(buf[:])
}
