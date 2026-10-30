package main

import (
	"FairGate/internal/config"
	"FairGate/internal/shipper"
	"FairGate/internal/wal"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
)

const walUsage = `usage: fairgate wal inspect|repair [flags]

  inspect  read every WAL segment, list damaged regions and what startup would do with each.
           Exits 0 when startup would succeed, 1 when it would refuse, 2 on error.
  repair   quarantine damaged regions that are not shipped and re-append the valid records
           found after them, so startup succeeds. Stop the gateway first.

flags:
`

// runWAL runs "fairgate wal ..." and returns the exit status.
func runWAL(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || (args[0] != "inspect" && args[0] != "repair") {
		fmt.Fprint(stderr, walUsage)
		return 2
	}
	command := args[0]

	flags := flag.NewFlagSet("wal "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprint(stderr, walUsage)
		flags.PrintDefaults()
	}
	configPath := flags.String("config", os.Getenv("FAIRGATE_CONFIG"), "gateway config file; wal.path and shipper.checkpoint_path come from it")
	walPath := flags.String("wal", "", "WAL path (overrides the config)")
	checkpointPath := flags.String("checkpoint", "", "shipper checkpoint path (overrides the config)")
	asJSON := flags.Bool("json", false, "print the report as JSON")
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, "fairgate wal:", err)
		return 2
	}
	if *walPath == "" {
		*walPath = cfg.WAL.Path
	}
	if *checkpointPath == "" {
		*checkpointPath = cfg.Shipper.CheckpointPath
	}
	checkpoint, err := shipper.NewCheckpointStore(*checkpointPath).Load()
	if err != nil {
		fmt.Fprintln(stderr, "fairgate wal: load checkpoint:", err)
		return 2
	}
	shipped := wal.Position{SegmentId: checkpoint.Segment, Offset: checkpoint.Offset}

	if command == "inspect" {
		return inspectWAL(*walPath, shipped, *asJSON, stdout, stderr)
	}
	return repairWAL(*walPath, shipped, *asJSON, stdout, stderr)
}

func inspectWAL(path string, shipped wal.Position, asJSON bool, stdout, stderr io.Writer) int {
	reports, err := wal.Inspect(path, shipped)
	if err != nil {
		fmt.Fprintln(stderr, "fairgate wal inspect:", err)
		return 2
	}
	refused := false
	for _, report := range reports {
		for _, damage := range report.Damage {
			refused = refused || damage.Verdict == wal.VerdictUnshipped
		}
	}

	if asJSON {
		writeJSON(stdout, map[string]any{"checkpoint": shipped, "segments": reports, "startup_refuses": refused})
	} else {
		fmt.Fprintf(stdout, "checkpoint: segment %d offset %d\n", shipped.SegmentId, shipped.Offset)
		for _, report := range reports {
			fmt.Fprintf(stdout, "segment %d  %s  %s  %d bytes  %d records\n",
				report.Id, report.Format, report.Path, report.Size, report.Records)
			for _, damage := range report.Damage {
				fmt.Fprintf(stdout, "  damaged bytes %d-%d: %s -> %s\n", damage.Start, damage.End, damage.Reason, describeVerdict(damage.Verdict))
			}
		}
		if refused {
			fmt.Fprintln(stdout, "startup will refuse: unshipped records are damaged. Run \"fairgate wal repair\" with the gateway stopped, or set wal.on_corruption: quarantine.")
		} else {
			fmt.Fprintln(stdout, "startup will succeed")
		}
	}
	if refused {
		return 1
	}
	return 0
}

func describeVerdict(verdict string) string {
	switch verdict {
	case wal.VerdictTruncatedTail:
		return "torn final write, truncated on startup"
	case wal.VerdictSkippedShipped:
		return "already shipped, skipped on startup"
	default:
		return "NOT SHIPPED: startup refuses; repair quarantines it"
	}
}

func repairWAL(path string, shipped wal.Position, asJSON bool, stdout, stderr io.Writer) int {
	recovery, err := wal.Repair(path, shipped)
	if err != nil {
		fmt.Fprintln(stderr, "fairgate wal repair:", err)
		return 2
	}
	if asJSON {
		writeJSON(stdout, map[string]any{"damage": recovery.Damage, "salvaged": len(recovery.Salvaged), "events": len(recovery.Events)})
		return 0
	}
	for _, damage := range recovery.Damage {
		line := damage.String()
		if damage.QuarantinePath != "" {
			line += ", original copied to " + damage.QuarantinePath
		}
		fmt.Fprintln(stdout, line)
	}
	fmt.Fprintf(stdout, "repaired: %d valid records, %d salvaged from quarantined segments and appended again\n",
		len(recovery.Events), len(recovery.Salvaged))
	return 0
}

func writeJSON(w io.Writer, value any) {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}
