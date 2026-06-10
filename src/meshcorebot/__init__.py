"""MeshcoreBot — declarative scheduled tasks for MeshCore companion nodes.

Built as a fork of meshcore-cli (https://github.com/meshcore-dev/meshcore-cli),
preserving the original `meshcli` REPL for interactive debugging while adding
`meshcorebot` — a YAML-driven daemon that runs scheduled tasks like periodic
channel probes and trace loops over USB serial or BLE.

Version history (rough):
  0.1.0 — MVP: chan_msg + trace_loop tasks, BLE/USB transport, console+jsonl+mqtt sinks.
  0.1.1 — trace_matrix task: cycle-based MY→OTHER→MY probing with cumulative
          summary table. BLE bringup: name-substring picker, active scan for
          long names, disconnect-driven reconnect. Stats persistence (in-memory
          default; -p/--persistence for on-disk; -r/--reset to wipe).
  0.2.0 — Multi-config CLI: `meshcorebot a.yaml b.yaml` merges tasks from
          multiple files onto a single companion (transports must match;
          task names must be unique OR reconciled by enabled flag: both
          disabled → dedupe, one enabled + one disabled → enabled wins).
          New `bot.cross_task_delay` (default 2s) — global gate enforcing a
          minimum gap between any two send_trace BLE events across tasks;
          MAX-merge semantics across configs (strictest gap wins).
          Send-to-send cadence: trace_delay is now measured from send-start
          to next send-start (not from response/timeout) — packets fly on a
          fixed rhythm even when traces time out. cycle_interval is similarly
          measured cycle-start to cycle-start. Both auto-bump if too small
          relative to timeout + cross_task_delay or to N×trace_delay.
          Cycle barrier syncs end-of-cycle across trace_matrix tasks so
          their summaries print back-to-back with no interleaved trace
          events from a slower sibling. Compact console summary header
          ("[ts] task (cycle N)") folds the redundant title into the event
          line. Timestamps now in local timezone; HH:MM:SS extractor fixed
          (old rstrip was eating trailing zeros — '14:37:20' → '14:37:2').
          BLE disconnect capped at 3s to prevent shutdown hang. faulthandler
          on SIGUSR1 for live stack dumps during hangs.
"""

__version__ = "0.2.0"
