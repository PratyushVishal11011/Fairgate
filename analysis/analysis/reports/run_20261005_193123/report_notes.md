# FairGate benchmark report notes

- Run `run_20261005_193123` contains 50 producer summaries and 125,000 ACK-latency records.
- ACK round-trip latency median was 75.16 ms; P95 was 98.81 ms and P99 was 118.96 ms.
- Of 125,000 ACKed events, 125,000 were accepted and 0 rejected (100.00% accepted among ACKed events).
- Median ACK latency during the anomaly phase was 1.02× the baseline median.
- Jain's fairness index across per-producer throughput rates was 0.999; interpret this as throughput equality for this run, not as proof of scheduler fairness under all workloads.
- These latency values are client-observed ACK round-trip times, not ClickHouse insert or query latency. ACKs indicate WAL persistence and queue admission.
