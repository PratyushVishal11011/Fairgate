# FairGate benchmark report notes

- Run `run_20261005_193123` contains 50 producers and 125,000 ACK-latency observations.
- Median ACK round-trip latency was 75.16 ms; P95 was 98.81 ms and P99 was 118.96 ms.
- 125,000 of 125,000 ACKed events were accepted (100.00%). Producer summaries also record 0 errors.
- Anomaly-phase median ACK latency was 1.02× the baseline-phase median.
- Jain’s index for observed per-producer throughput was 0.999; this measures equality in this run, not scheduler fairness in general.
- Across 3 anomaly-assigned producer(s) with baseline and anomaly data, the median producer-level change in median latency was 3.17% (range -0.25% to 4.17%).
- 6,250 ACK observations were above the run-wide P95 value of 98.81 ms; compare profile-level rates in the exceedance table.
- These latency values are client-observed ACK round-trip times, not ClickHouse insert latency. ACKs indicate WAL persistence and queue admission.
