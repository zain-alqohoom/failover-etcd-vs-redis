# The True Cost of Failover: etcd (CP) vs Redis (AP)

An empirical study of leader-failover costs in distributed coordination systems, conducted as part of the **Advanced Operating Systems** graduate course.

**Author:** Zain Al-Abideen Ali Al-Qahoom  
**Program:** Master of Computing  
**University:** University of Science and Technology — Yemen  
**Supervisor:** Dr. Widad Al-Sarouri  
**Course:** Advanced Operating Systems

---

## 📋 Overview

This project compares two distributed systems under identical randomized `kill-the-leader` workloads:

| System | Model | Coordination | Replication |
|--------|-------|--------------|-------------|
| etcd | CP (Raft) | Automatic election | Synchronous (quorum) |
| Redis | AP-ish | None (manual) | Asynchronous |

**Research Question:** When a leader dies mid-write, how long can clients write again — and what does tuning the failure detector cost?

---

## 📊 Key Results

| Metric | Redis (AP-ish) | etcd Run1 (100ms/1s) | etcd Run2 (500ms/5s) |
|--------|----------------|----------------------|----------------------|
| **Election-obs. p50** | N/A | **1,208 ms** | 6,772 ms |
| **Election-obs. p99** | N/A | **1,839 ms** | 9,483 ms |
| **Unavailability p50** | 13,082 ms | **2,013 ms** | 8,051 ms |
| **Unavailability p99** | 19,025 ms | **2,582 ms** | 15,044 ms |
| **Events detected** | 20/20 | 20/20 | 20/20 |
| **Lost writes (same-master restart)** | 0 / 30,767 | N/A (CP) | N/A (CP) |

### Headline Findings
1. **CP recovers 6.5× faster at the median** (2.01s vs 13.08s).
2. **Tail latency gap is even larger**: etcd Run1 p99 = 2.58s vs Redis p99 = 19.03s (7.4×).
3. **Failure-detector tuning has a non-linear cost**: 5× timeout increase produces 4× median and **5.8× tail** degradation.
4. **Failover cost decomposes cleanly**: `Unavailability ≈ Election-observation time + Client reconnect overhead` (~0.8–1.3s).
5. **Zero write loss observed** in our Redis setup — a consequence of same-master restart, not proof of safety (see *Threats to Validity*).

---

## 🗂️ Repository Structure

```
.
├── docker-compose.yml                    # Cluster definition (3× etcd + 3× Redis)
├── run_experiment.py                     # Main orchestrator: workload + kill + timing + analysis
├── analyze_only.py                       # Re-analyze a CSV without re-running the experiment
├── .env                                  # etcd tuning parameters (heartbeat/election)
├── README.md
├── report.tex                            # IEEE LaTeX source
├── Report.pdf                            # 4-page IEEE-format report
├── redis_results.csv                     # Per-write client-side log
│                                         #   (30,835 total ops: 30,767 success + 68 failed)
├── etcd_results_run1.csv                 # Per-write client-side log (25,574 writes)
├── etcd_results_run2.csv                 # Per-write client-side log (33,510 writes)
├── etcd_results_run1_events.csv          # Event log: kill timestamps + election-observation timing
├── etcd_results_run2_events.csv          # Event log: kill timestamps + election-observation timing
├── redis_cdf.png                         # CDF of unavailability — Redis
├── etcd_cdf_run1.png                     # CDF of unavailability — etcd Run1
└── etcd_cdf_run2.png                     # CDF of unavailability — etcd Run2
```

---

## ⚙️ Requirements

- Docker ≥ 20.10 and Docker Compose ≥ 1.29
- Python ≥ 3.8
- Python packages: `redis`, `etcd3`, `pandas`, `numpy`, `matplotlib`, `protobuf==3.20.3`

---

## 🚀 Quick Start

### 1. Clone and set up

```bash
git clone <your-repo-url>
cd "The true cost of failover"

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install redis etcd3 pandas numpy matplotlib "protobuf==3.20.3"
```

### 2. Start the clusters

```bash
docker-compose up -d
docker ps   # Verify 6 containers are running
```

### 3. Run the experiment

**Redis (AP-ish):**
```bash
python run_experiment.py redis redis_results.csv
```

**etcd with aggressive timeouts (100ms / 1s):**
```bash
# Set .env:
printf "ETCD_HEARTBEAT_INTERVAL=100\nETCD_ELECTION_TIMEOUT=1000\n" > .env
docker-compose down -v
docker-compose up -d --force-recreate
python run_experiment.py etcd etcd_results_run1.csv
```

**etcd with conservative timeouts (500ms / 5s):**
```bash
# Set .env:
printf "ETCD_HEARTBEAT_INTERVAL=500\nETCD_ELECTION_TIMEOUT=5000\n" > .env
docker-compose down -v
docker-compose up -d --force-recreate
python run_experiment.py etcd etcd_results_run2.csv
```

Each run takes **8–13 minutes** (20 randomized kills).

### 4. Re-analyze results without re-running

```bash
python analyze_only.py <results.csv> <redis|etcd>
```

Outputs:
- p50 / p99 client-visible unavailability
- Number of failure events detected
- CDF plot (`.png`)
- p50 / p99 election-observation time (etcd only, from `*_events.csv`)

---

## 🧪 Experimental Design

### Infrastructure
- **Container runtime:** Docker (bridge network)
- **etcd:** `quay.io/coreos/etcd:v3.5.9` — 3-node Raft cluster
- **Redis:** `redis:7.2-alpine` — 1 master + 2 async replicas, `appendonly yes`

### Honest Kill Protocol
Each iteration:
1. Random stabilization wait: `U(3, 20)s`
2. Discover current leader (dynamic for etcd, static for Redis)
3. `docker kill -s SIGKILL <leader>` (≡ `kill -9`)
4. **Failover tracking (etcd only):** active polling every 200 ms until a different node becomes leader
5. `docker start <leader>`
6. Random post-recovery wait: `U(3, 7)s`

Repeated **20 times** per configuration. All timing is captured in `*_events.csv`.

### Workload
Python client writing ~100 ops/s, capturing `t_start` / `t_end` per request.

### Metrics
- **Election-observation time** (etcd only): end-to-end failover-observation time from kill to first observation of a different leader. Includes failure detection, leader election, and 200-ms polling granularity. **Not** the internal Raft election time.
- **Client-visible unavailability**: last successful ACK before failure → first successful ACK after recovery.
- **Failure events**: distinct intervals where `status == FAILED`.
- **Lost writes** (Redis): ACKed writes not present in master after recovery. Reported in the main `run_experiment.py` output (same-master-restart configuration).

---

## 🔬 Theoretical Framework

| Concept | Role in this Experiment |
|---------|-------------------------|
| **CAP Theorem** | Explains CP (etcd) vs AP-ish (Redis) trade-off |
| **FLP Impossibility** | Justifies why neither system can be both safe and live |
| **Accrual Failure Detection (φ)** | The `heartbeat/election-timeout` pair is the suspicion threshold |

### Key Equation

```
Unavailability ≈ Election-observation time + Client reconnect overhead
```

**Empirical decomposition:**
- Run1: 2.01s ≈ 1.21s (election-obs.) + 0.80s (reconnect)
- Run2: 8.05s ≈ 6.77s (election-obs.) + 1.28s (reconnect)

---

## ⚠️ Threats to Validity

- **Zero lost writes in Redis** is due to our `appendonly` configuration and *same-master restart*, not proof of safety under permanent failure. A proper test would promote a replica (via Sentinel or manual) and compare its dataset against the client's log.
- **Docker `kill -9`** ≠ hardware crash; no network partitions, disk faults, or asymmetric latency are simulated.
- **Single-host deployment**: tail latency is likely inflated by CPU contention during simultaneous restarts.
- **n = 20 events**: p99 ≈ max observed value; percentile interpolation can shift p99 by up to one sample.

See Section VI of the report for a complete discussion.

---

## 📚 References

1. D. Ongaro and J. Ousterhout. *In Search of an Understandable Consensus Algorithm (Raft)*. USENIX ATC, 2014.
2. S. Gilbert and N. Lynch. *Brewer's Conjecture and the Feasibility of Consistent, Available, Partition-Tolerant Web Services*. ACM SIGACT News, 2002.
3. M. J. Fischer, N. A. Lynch, and M. S. Paterson. *Impossibility of Distributed Consensus with One Faulty Process*. JACM, 1985.
4. N. Hayashibara et al. *The φ Accrual Failure Detector*. IEEE SRDS, 2004.
5. M. Kleppmann. *Designing Data-Intensive Applications*. O'Reilly, 2017.

---

## 📄 License

This project is released for academic use only.

---

## 👤 Author

**Zain Al-Abideen Ali Al-Qahoom**  
Master of Computing — University of Science and Technology, Yemen  
Supervisor: **Dr. Widad Al-Sarouri**  
Course: Advanced Operating Systems