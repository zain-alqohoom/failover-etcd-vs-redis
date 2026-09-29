import subprocess, time, random, json, sys, threading, csv
import redis, etcd3
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

REDIS_HOST, ETCD_HOST, ETCD_PORT = 'localhost', 'localhost', 2379
ITERATIONS = 20

# ========== WORKLOAD ==========
class WorkloadGenerator:
    def __init__(self, system):
        self.system = system
        self.running = False
        self.results = []
        if system == 'redis':
            self.client = redis.Redis(host=REDIS_HOST, port=6379, socket_timeout=2)
        else:
            self.client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT, timeout=2)

    def run(self):
        self.running = True
        counter = 0
        while self.running:
            key = f"key_{counter}"
            value = f"value_{counter}_{random.randint(1000,9999)}"
            t_start = time.time()
            status, err = "SUCCESS", ""
            try:
                if self.system == 'redis':
                    self.client.set(key, value)
                else:
                    self.client.put(key, value)
            except Exception as e:
                status, err = "FAILED", str(e)
            t_end = time.time()
            self.results.append({
                'timestamp': datetime.now().isoformat(), 'key': key, 'value': value,
                't_start': t_start, 't_end': t_end,
                'latency_ms': (t_end - t_start) * 1000,
                'status': status, 'error': err
            })
            counter += 1
            time.sleep(0.01)

    def stop(self):
        self.running = False

    def save(self, filename):
        with open(filename, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=['timestamp','key','value','t_start','t_end','latency_ms','status','error'])
            w.writeheader()
            w.writerows(self.results)
        print(f"[*] Saved {len(self.results)} records to {filename}")

# ========== EVENT LOGGER ==========
class EventLogger:
    def __init__(self, filename):
        self.filename = filename
        with open(filename, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['iteration', 'event', 'wall_clock', 'elapsed_ms',
                        'leader_before', 'leader_after', 'details'])

    def log(self, iteration, event, elapsed_ms=0, leader_before="", leader_after="", details=""):
        wall = datetime.now().isoformat()
        with open(self.filename, 'a', newline='') as f:
            w = csv.writer(f)
            w.writerow([iteration, event, wall, f"{elapsed_ms:.2f}",
                        leader_before, leader_after, details])

# ========== LEADER DISCOVERY (FIXED) ==========
def get_etcd_leader(timeout=2):
    """Query each etcd container individually to find the leader."""
    nodes = {}
    for container in ['etcd1', 'etcd2', 'etcd3']:
        try:
            cmd = f"docker exec {container} etcdctl endpoint status --write-out=json"
            r = subprocess.run(cmd, shell=True, capture_output=True,
                               text=True, timeout=timeout)
            if r.returncode != 0:
                continue
            data = json.loads(r.stdout)
            for ep in data:
                status = ep.get('Status', {})
                header = status.get('header', {})
                member_id = str(header.get('member_id', ''))
                leader_id = str(status.get('leader', ''))
                if member_id and leader_id:
                    nodes[container] = {'member_id': member_id, 'leader_id': leader_id}
        except Exception:
            continue

    for container, info in nodes.items():
        if info['member_id'] == info['leader_id']:
            return container
    return None


def wait_for_new_leader(old_leader, timeout=15, poll_interval=0.2):
    """Poll each container individually until a new (different) leader appears."""
    start = time.time()
    while (time.time() - start) < timeout:
        current = get_etcd_leader()
        if current and current != old_leader:
            return current, (time.time() - start) * 1000
        time.sleep(poll_interval)
    return None, (time.time() - start) * 1000


def kill_container(name):
    subprocess.run(f"docker kill -s SIGKILL {name}", shell=True, check=True)


def start_container(name):
    subprocess.run(f"docker start {name}", shell=True, check=True)

# ========== ANALYZER ==========
def analyze_unavailability(csv_file):
    df = pd.read_csv(csv_file, low_memory=False)
    df['is_failed'] = df['status'] == 'FAILED'
    failures, in_fail, start_fail = [], False, 0
    for _, row in df.iterrows():
        if row['is_failed'] and not in_fail:
            in_fail, start_fail = True, row['t_start']
        elif not row['is_failed'] and in_fail:
            in_fail = False
            failures.append(row['t_start'] - start_fail)
    if not failures:
        print("[!] No failures detected.")
        return np.array([])
    failures = np.array(failures) * 1000
    print(f"\n[RESULT] Unavailability: p50={np.percentile(failures,50):.2f}ms, p99={np.percentile(failures,99):.2f}ms")
    return failures


def plot_cdf(data, system, filename):
    s = np.sort(data)
    y = np.arange(len(s)) / float(len(s) - 1)
    plt.figure(figsize=(8,5))
    plt.plot(s, y, marker='.', linestyle='none')
    plt.title(f'CDF of Unavailability - {system}')
    plt.xlabel('Unavailability (ms)'); plt.ylabel('CDF'); plt.grid(True)
    plt.savefig(filename); plt.close()
    print(f"[*] Saved CDF plot to {filename}")


def analyze_lost_writes(csv_file):
    df = pd.read_csv(csv_file, low_memory=False)
    ok = df[df['status'] == 'SUCCESS']
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    lost = sum(1 for _, row in ok.iterrows() if r.get(row['key']) != row['value'])
    pct = (lost/len(ok)*100) if len(ok) > 0 else 0
    print(f"[RESULT] Redis lost writes (same-master restart): {lost}/{len(ok)} ({pct:.2f}%)")


def analyze_events(events_file):
    try:
        df = pd.read_csv(events_file)
    except Exception as e:
        print(f"[!] Could not read events file: {e}")
        return
    kills = df[df['event'] == 'kill']
    elections = df[df['event'] == 'election_complete']
    print(f"\n[EVENTS] Total kills logged: {len(kills)}")
    print(f"[EVENTS] Total elections completed: {len(elections)}")
    if len(elections) > 0:
        et = elections['elapsed_ms'].astype(float).values
        print(f"[EVENTS] Election time (ms): p50={np.percentile(et,50):.2f}, p99={np.percentile(et,99):.2f}, max={et.max():.2f}")

# ========== MAIN ==========
def main():
    if len(sys.argv) != 3:
        print("Usage: python run_experiment.py <redis|etcd> <output_csv>")
        sys.exit(1)
    system, output = sys.argv[1], sys.argv[2]
    events_file = output.replace('.csv', '_events.csv')

    print(f"\n{'='*60}\n  FAILOVER EXPERIMENT: {system.upper()}\n{'='*60}\n")

    logger = EventLogger(events_file)

    print("[PHASE 1] Starting workload...")
    gen = WorkloadGenerator(system)
    t = threading.Thread(target=gen.run); t.start()
    print("[*] Workload running.\n")

    print(f"[PHASE 2] {ITERATIONS} COMPLETELY RANDOM kills\n")
    successful_kills = 0
    for i in range(1, ITERATIONS + 1):
        wait = random.uniform(3, 20)
        print(f"--- Kill #{i}/{ITERATIONS} (in {wait:.2f}s) ---")
        time.sleep(wait)

        if system == 'etcd':
            target = get_etcd_leader()
            if not target:
                print("    [!] No leader found, skipping")
                logger.log(i, "kill_skipped", 0, details="no_leader_found")
                continue
        else:
            target = "redis-master"

        # ---- Kill phase ----
        kill_start = time.time()
        print(f"    [!] Killing {target} with SIGKILL")
        kill_container(target)
        successful_kills += 1
        logger.log(i, "kill", 0, leader_before=target, details="SIGKILL sent")

        # ---- Detection/Election phase ----
        if system == 'etcd':
            new_leader, election_ms = wait_for_new_leader(target, timeout=15)
            logger.log(i, "election_complete", election_ms,
                       leader_before=target, leader_after=new_leader or "unknown",
                       details=f"detected_new_leader={new_leader}")
            print(f"    [*] New leader: {new_leader} (election time: {election_ms:.2f} ms)")
        else:
            time.sleep(random.uniform(5, 10))
            logger.log(i, "restart", (time.time()-kill_start)*1000,
                       leader_before=target, leader_after=target)

        # ---- Restart phase ----
        print(f"    [*] Restarting {target}")
        start_container(target)
        time.sleep(random.uniform(3, 7))

    print(f"\n[*] Total successful kills: {successful_kills}/{ITERATIONS}")

    print("\n[PHASE 3] Stopping workload...")
    gen.stop(); t.join(); gen.save(output)

    print("\n[PHASE 4] Analyzing...")
    un = analyze_unavailability(output)
    if len(un) > 0:
        plot_cdf(un, system, f"{system}_cdf.png")
    if system == 'redis':
        analyze_lost_writes(output)

    analyze_events(events_file)
    print(f"\n{'='*60}\n  EXPERIMENT COMPLETE!\n{'='*60}\n")


if __name__ == "__main__":
    main()