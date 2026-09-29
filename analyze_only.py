import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import redis

def analyze_unavailability(csv_file):
    df = pd.read_csv(csv_file)
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
    print(f"[RESULT] Unavailability: p50={np.percentile(failures,50):.2f}ms, p99={np.percentile(failures,99):.2f}ms")
    print(f"[INFO] Failure events detected: {len(failures)}")
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

def analyze_events(events_file):
    try:
        df = pd.read_csv(events_file)
    except Exception as e:
        print(f"[!] Could not read events file: {e}")
        return
    kills = df[df['event'] == 'kill']
    elections = df[df['event'] == 'election_complete']
    print(f"\n[EVENTS] Kills logged: {len(kills)}")
    if len(elections) > 0:
        et = elections['elapsed_ms'].astype(float).values
        print(f"[EVENTS] Election time (ms): p50={np.percentile(et,50):.2f}, p99={np.percentile(et,99):.2f}, max={et.max():.2f}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python analyze_only.py <csv_file> <redis|etcd>")
        sys.exit(1)
    csv_file, system = sys.argv[1], sys.argv[2]
    print(f"\n=== Analyzing {system} results ===\n")
    un = analyze_unavailability(csv_file)
    if len(un) > 0:
        plot_cdf(un, system, f"{system}_cdf.png")
    analyze_events(csv_file.replace('.csv', '_events.csv'))