# Debugging FODC

This guide explains how to debug FODC functions and components.

## Prerequisites

1. **Go Debugger (Delve)**: Ensure you have Delve installed:
   ```bash
   go install github.com/go-delve/delve/cmd/dlv@latest
   ```

2. **VS Code Go Extension**: Make sure you have the Go extension installed in your editor.

## Debugging Methods

### Method 1: Using VS Code Debugger (Recommended)

1. **Open the Debug Panel**: Press `F5` or click the Debug icon in the sidebar
2. **Select Configuration**: Choose one of the debug configurations:
   - **Debug FODC CLI**: Runs with default arguments
   - **Debug FODC CLI (Minimal Args)**: Runs with minimal/no arguments
   - **Debug FODC Tests**: Runs all tests in debug mode
   - **Attach to FODC Process**: Attach to a running process

3. **Set Breakpoints**: Click in the gutter next to line numbers to set breakpoints
4. **Start Debugging**: Press `F5` to start debugging

#### Customizing Debug Arguments

Edit `.vscode/launch.json` to modify the `args` array for different configurations:

```json
"args": [
    "--metrics-url=http://localhost:2121/metrics",
    "--poll-interval=5s",
    "--health-url=http://localhost:17913/api/healthz"
]
```

### Method 2: Using Delve Command Line

#### Debug the main program:

```bash
cd fodc
dlv debug ./cmd/cli -- \
    --metrics-url=http://localhost:2121/metrics \
    --poll-interval=5s \
    --health-url=http://localhost:17913/api/healthz \
    --death-rattle-path=/tmp/death-rattle \
    --container=banyandb \
    --alert-threshold=0.8
```

#### Debug with breakpoints:

```bash
dlv debug ./cmd/cli
(dlv) break main.main
(dlv) break fodc/internal/poller/poller.go:50
(dlv) continue
(dlv) next
(dlv) print variableName
(dlv) continue
```

#### Debug a specific function:

```bash
dlv debug ./cmd/cli
(dlv) break poller.NewMetricsPoller
(dlv) continue
```

### Method 3: Debugging Individual Components

#### Debug Poller:

Set breakpoints in `fodc/internal/poller/poller.go`:
- `NewMetricsPoller()` - Constructor
- `Start()` - Main polling loop
- `pollMetrics()` - Individual poll

#### Debug Detector:

Set breakpoints in `fodc/internal/detector/`:
- `deathrattle.go` - Death rattle detection logic
- `alerts.go` - Alert analysis
- `container.go` - Container monitoring

#### Debug Flight Recorder:

Set breakpoints in `fodc/internal/flightrecorder/flightrecorder.go`:
- `NewFlightRecorder()` - Initialization
- `Record()` - Recording snapshots
- `ReadAll()` - Recovery logic

### Method 4: Debugging Tests

#### Run tests with debugger:

```bash
cd fodc
dlv test ./...
```

Or use VS Code: Select "Debug FODC Tests" configuration and press F5.

#### Debug a specific test:

```bash
dlv test ./internal/poller -- -run TestMetricsPoller
```

### Method 5: Using Print Statements

For quick debugging, add log statements:

```go
import "log"

func someFunction() {
    log.Printf("DEBUG: Variable value: %v", variable)
    log.Printf("DEBUG: Function called with args: %+v", args)
}
```

### Method 6: Attach to Running Process

1. **Start FODC in debug mode**:
   ```bash
   dlv exec ./build/bin/dev/fodc-cli --headless --listen=:2345 --api-version=2 -- \
       --metrics-url=http://localhost:2121/metrics
   ```

2. **Attach from VS Code**: Use "Attach to FODC Process" configuration

3. **Or attach with Delve**:
   ```bash
   dlv attach <PID> --headless --listen=:2345 --api-version=2
   ```

## Common Debugging Scenarios

### Debugging Metrics Polling

1. Set breakpoint in `poller/poller.go` at `pollMetrics()`
2. Check `metrics` variable to see parsed Prometheus data
3. Step through parsing logic

### Debugging Death Rattle Detection

1. Set breakpoint in `detector/deathrattle.go` at `checkDeathRattleFiles()`
2. Create a test file: `echo "test" > /tmp/death-rattle`
3. Watch the detection logic trigger

### Debugging Alert Generation

1. Set breakpoint in `detector/alerts.go` at `AnalyzeMetrics()`
2. Inspect `snapshot` variable
3. Step through threshold checks

### Debugging Flight Recorder

1. Set breakpoint in `flightrecorder/flightrecorder.go` at `Record()`
2. Check `snapshot` data being recorded
3. Test recovery by restarting and checking `ReadAll()`

## Tips

1. **Use conditional breakpoints**: Right-click a breakpoint → Edit Breakpoint → Add condition
2. **Watch variables**: Add variables to the Watch panel
3. **Call stack**: Use the Call Stack panel to see execution flow
4. **Debug console**: Evaluate expressions in the Debug Console
5. **Step over (F10)**: Execute current line
6. **Step into (F11)**: Enter function calls
7. **Step out (Shift+F11)**: Exit current function
8. **Continue (F5)**: Resume execution

## Troubleshooting

### "Cannot find package" errors

Run:
```bash
go mod download
go mod tidy
```

### Debugger not attaching

Ensure Delve is installed:
```bash
go install github.com/go-delve/delve/cmd/dlv@latest
```

### Port already in use

Change the port in launch.json or use a different port:
```json
"port": 2346
```

## Example: Debugging a Specific Issue

Let's say you want to debug why alerts aren't being triggered:

1. Set breakpoint at `detector/alerts.go:AnalyzeMetrics()`
2. Start debugging with "Debug FODC CLI"
3. When breakpoint hits, inspect:
   - `snapshot.ErrorRate`
   - `snapshot.MemoryUsage`
   - `snapshot.DiskUsage`
4. Check if thresholds are being met
5. Step through each alert condition

## Resources

- [Delve Documentation](https://github.com/go-delve/delve)
- [VS Code Go Debugging](https://github.com/golang/vscode-go/wiki/debugging)
- [Go Debugging Guide](https://go.dev/doc/diagnostics)

