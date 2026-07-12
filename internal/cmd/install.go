package cmd

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/blaspat/flare/internal/term"
)

const nssmVersion = "2.24"
const nssmDownloadURL = "https://nssm.cc/release/nssm-%s.zip"

// windowsServiceName is the NSSM service name for Flare.
const windowsServiceName = "Flare"

// windowsEnsureNSSM downloads NSSM alongside flare.exe if not found,
// then installs the Flare Windows service. Returns the nssm.exe path.
// On non-Windows platforms this is a no-op that returns ("", nil).
func windowsEnsureNSSM() (string, error) {
	if runtime.GOOS != "windows" {
		return "", nil
	}

	exePath, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("cannot determine executable path: %w", err)
	}
	dir := filepath.Dir(exePath)
	nssmPath := filepath.Join(dir, "nssm.exe")

	// 1. Download NSSM if not present alongside the binary
	if _, err := os.Stat(nssmPath); os.IsNotExist(err) {
		fmt.Println(term.Yellow + "NSSM (Non-Sucking Service Manager) not found." + term.Reset)
		if err := downloadNSSM(dir); err != nil {
			return "", fmt.Errorf("download NSSM: %w", err)
		}
		fmt.Println(term.Green + "NSSM downloaded." + term.Reset)
	}

	// 2. Check if the Flare service is already installed
	if serviceInstalled(nssmPath) {
		return nssmPath, nil
	}

	// 3. Install the service
	fmt.Print("Installing Flare Windows service... ")
	if err := installService(nssmPath, exePath, dir); err != nil {
		return "", fmt.Errorf("install service: %w", err)
	}
	fmt.Println(term.Green + "done." + term.Reset)

	// 4. Set it to auto-start
	_ = exec.Command(nssmPath, "set", windowsServiceName, "Start", "SERVICE_AUTO_START").Run()

	return nssmPath, nil
}

// windowsStartService starts the Flare Windows service and waits briefly.
// Returns true if the service is running after the attempt.
func windowsStartService() bool {
	if runtime.GOOS != "windows" {
		return false
	}
	out, err := exec.Command("net", "start", windowsServiceName).CombinedOutput()
	if err != nil {
		// Might already be running — check status
		status, _ := exec.Command("sc", "query", windowsServiceName).Output()
		return strings.Contains(string(status), "RUNNING")
	}
	fmt.Println(string(out))
	return strings.Contains(string(out), "started") || strings.Contains(string(out), "already")
}

// windowsStopService stops the Flare Windows service.
func windowsStopService() error {
	if runtime.GOOS != "windows" {
		return nil
	}
	out, err := exec.Command("net", "stop", windowsServiceName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("stop service: %s", strings.TrimSpace(string(out)))
	}
	fmt.Println(string(out))
	return nil
}

// --- internal helpers -------------------------------------------------------

func downloadNSSM(dir string) error {
	url := fmt.Sprintf(nssmDownloadURL, nssmVersion)

	fmt.Printf("  Downloading %s ...\n", url)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	// Determine which arch to extract
	archDir := "win32"
	if runtime.GOARCH == "amd64" {
		archDir = "win64"
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}

	found := false
	for _, f := range zipReader.File {
		if f.FileInfo().IsDir() {
			continue
		}
		// Look for e.g. "nssm-2.24/win64/nssm.exe"
		if !strings.HasSuffix(f.Name, "nssm.exe") {
			continue
		}
		if !strings.Contains(f.Name, "/"+archDir+"/") {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("open %s in zip: %w", f.Name, err)
		}
		defer rc.Close()

		outPath := filepath.Join(dir, "nssm.exe")
		outFile, err := os.Create(outPath)
		if err != nil {
			return fmt.Errorf("create %s: %w", outPath, err)
		}
		defer outFile.Close()

		if _, err := io.Copy(outFile, rc); err != nil {
			return fmt.Errorf("write %s: %w", outPath, err)
		}
		found = true
		break
	}

	if !found {
		return fmt.Errorf("nssm.exe for %s not found in zip archive", archDir)
	}
	return nil
}

func serviceInstalled(nssmPath string) bool {
	out, err := exec.Command(nssmPath, "get", windowsServiceName, "AppDirectory").Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) != ""
}

func installService(nssmPath, exePath, dir string) error {
	// Create logs directory
	logDir := filepath.Join(dir, "logs")
	_ = os.MkdirAll(logDir, 0755)

	steps := []struct {
		args []string
		desc string
	}{
		{[]string{"install", windowsServiceName, exePath, "start"}, "install service"},
		{[]string{"set", windowsServiceName, "AppDirectory", dir}, "set working directory"},
		{[]string{"set", windowsServiceName, "AppStdout", filepath.Join(logDir, "stdout.log")}, "set stdout log"},
		{[]string{"set", windowsServiceName, "AppStderr", filepath.Join(logDir, "stderr.log")}, "set stderr log"},
		{[]string{"set", windowsServiceName, "AppEnvironmentExtra", "_FLARE_SERVICE=1"}, "set service env"},
	}

	// Try setting AppNoConsole (may fail on older NSSM versions — non-fatal)
	steps = append(steps, struct{ args []string; desc string }{
		[]string{"set", windowsServiceName, "AppNoConsole", "1"}, "hide console window",
	})

	for _, step := range steps {
		cmd := exec.Command(nssmPath, step.args...)
		cmd.Stdout = nil
		cmd.Stderr = nil
		if err := cmd.Run(); err != nil {
			// AppNoConsole is best-effort
			if strings.Contains(step.desc, "console") {
				continue
			}
			return fmt.Errorf("%s: %w", step.desc, err)
		}
	}
	return nil
}

// windowsOpenBrowser opens the default browser to the given URL.
func windowsOpenBrowser(url string) {
	if runtime.GOOS != "windows" {
		return
	}
	// Try to open browser — best-effort, ignore errors
	_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
}

// installCmd runs 'flare install' — installs NSSM and sets up the Windows service.
func installCmd(cfgPath string) error {
	if runtime.GOOS != "windows" {
		return fmt.Errorf("'install' is only supported on Windows\n  On Linux: use systemd (see LINUX.md)\n  On macOS: use launchd (see MACOS.md)")
	}

	nssmPath, err := windowsEnsureNSSM()
	if err != nil {
		return fmt.Errorf("install: %w", err)
	}

	fmt.Printf("\n" + term.Green + term.Bold + "✓ Flare service installed!" + term.Reset + "\n")
	fmt.Printf("  Binary:     %s\n", os.Args[0])
	fmt.Printf("  NSSM:       %s\n", nssmPath)
	fmt.Printf("  Dashboard:  http://localhost:9722\n")
	fmt.Printf("  Logs:       %s\\logs\\\n", filepath.Dir(nssmPath))
	fmt.Printf("\n")
	fmt.Printf("  " + term.Bold + "Commands:" + term.Reset + "\n")
	fmt.Printf("    %s start       — start the service\n", os.Args[0])
	fmt.Printf("    %s stop        — stop the service\n", os.Args[0])
	fmt.Printf("    %s status      — show service status\n", os.Args[0])
	fmt.Printf("    %s uninstall   — remove the service\n", os.Args[0])
	fmt.Printf("\n")
	return nil
}

// stopCmd runs 'flare stop' — stops the Windows service.
func stopCmd() error {
	if runtime.GOOS != "windows" {
		return fmt.Errorf("'stop' is only supported on Windows\n  On Linux: use 'kill' or 'systemctl stop flare'\n  On macOS: use 'launchctl unload'")
	}
	return windowsStopService()
}

// uninstallCmd runs 'flare uninstall' — removes the Windows service.
func uninstallCmd() error {
	if runtime.GOOS != "windows" {
		return fmt.Errorf("'uninstall' is only supported on Windows\n  On Linux: use 'systemctl disable flare'\n  On macOS: use 'launchctl unload'")
	}

	// Stop the service first
	_ = windowsStopService()

	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("cannot determine executable path: %w", err)
	}
	dir := filepath.Dir(exePath)
	nssmPath := filepath.Join(dir, "nssm.exe")

	out, err := exec.Command(nssmPath, "remove", windowsServiceName, "confirm").CombinedOutput()
	if err != nil {
		return fmt.Errorf("remove service: %s", strings.TrimSpace(string(out)))
	}
	fmt.Printf("Flare service removed.\n")
	return nil
}
