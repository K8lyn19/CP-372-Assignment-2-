"""
reliability_test.py - Part B reliability tests (CP372 Assignment 2).

For each protocol (Stop-and-Wait, Go-Back-N) and each loss rate, starts the
receiver with --loss, sends test files with the matching sender, and checks
that the received file is byte-for-byte identical to the original (SHA-256).
Records timeouts, retransmissions, simulated drops, duplicates and
out-of-order packets for every run.

Outputs (in --results, default "reliability_results"):
  results.csv   one row per run
  summary.md    averages per protocol / loss rate / file size + pass count
  duplicate_check.md  scripted duplicate + out-of-order packets and the
                ACKs each receiver returned
  traces/       verbose logs of a small transfer at 30% loss, showing
                Stop-and-Wait resending one packet and GBN resending its window

Usage:
  python reliability_test.py
  python reliability_test.py --sizes 10KB,100KB,1MB --runs 3 --timeout 0.05
  python reliability_test.py --no-trace
"""

import argparse
import csv
import hashlib
import os
import queue
import re
import subprocess
import sys
import threading
import time
from collections import defaultdict

from rdt_common import parse_size

PROTOCOLS = {
    "stopwait": ("sender_stopwait.py", "receiver_stopwait.py"),
    "gbn": ("sender_gbn.py", "receiver_gbn.py"),
}
HERE = os.path.dirname(os.path.abspath(__file__))
PY = [sys.executable, "-u"]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def make_test_file(folder, size_text):
    """Random bytes, so any dropped, duplicated or reordered chunk changes the hash."""
    path = os.path.join(folder, f"test_{size_text}.bin")
    size = parse_size(size_text)
    if not (os.path.exists(path) and os.path.getsize(path) == size):
        with open(path, "wb") as f:
            f.write(os.urandom(size))
    return path


class Receiver:
    """Runs a receiver as a subprocess and collects its output lines."""

    def __init__(self, script, port, loss, out_dir, seed, log_path, verbose=False):
        cmd = PY + [os.path.join(HERE, script), "--port", str(port),
                    "--loss", str(loss), "--out", out_dir, "--seed", str(seed)]
        if verbose:
            cmd.append("-v")
        self.log = open(log_path, "w")
        self.lines = queue.Queue()
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True, cwd=HERE)
        threading.Thread(target=self._pump, daemon=True).start()
        self.wait_for("running", 10)

    def _pump(self):
        for line in self.proc.stdout:
            self.log.write(line)
            self.lines.put(line)

    def wait_for(self, text, timeout):
        """Return the next output line containing `text`, or None on timeout."""
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                line = self.lines.get(timeout=remaining)
            except queue.Empty:
                return None
            if text in line:
                return line

    def stop(self):
        self.proc.terminate()
        self.proc.wait()
        self.log.close()


def field(pattern, text, cast=float):
    m = re.search(pattern, text)
    return cast(m.group(1)) if m else None


def run_sender(script, path, port, timeout, window, log_path=None, verbose=False):
    cmd = PY + [os.path.join(HERE, script), "--file", path, "--port", str(port),
                "--timeout", str(timeout)]
    if script == "sender_gbn.py":
        cmd += ["--window", str(window)]
    if verbose:
        cmd.append("-v")
    out = subprocess.run(cmd, capture_output=True, text=True, cwd=HERE).stdout
    if log_path:
        with open(log_path, "w") as f:
            f.write(out)
    return out


def run_tests(args, results_dir):
    files_dir = os.path.join(results_dir, "files")
    os.makedirs(files_dir, exist_ok=True)
    sizes = [s.strip() for s in args.sizes.split(",")]
    losses = [float(x) for x in args.losses.split(",")]
    originals = {s: make_test_file(files_dir, s) for s in sizes}
    hashes = {s: sha256(p) for s, p in originals.items()}

    rows = []
    for proto, (sender, receiver) in PROTOCOLS.items():
        for loss in losses:
            out_dir = os.path.join(results_dir, f"received_{proto}")
            os.makedirs(out_dir, exist_ok=True)
            recv = Receiver(receiver, args.port, loss, out_dir, args.seed,
                            os.path.join(results_dir, f"receiver_{proto}_{int(loss*100)}.log"))
            try:
                for size in sizes:
                    for run in range(1, args.runs + 1):
                        received = os.path.join(out_dir, os.path.basename(originals[size]))
                        if os.path.exists(received):
                            os.remove(received)

                        out = run_sender(sender, originals[size], args.port,
                                         args.timeout, args.window)
                        stats_line = recv.wait_for("Dropped", 10) or ""

                        ok_transfer = "Transfer complete" in out
                        intact = (ok_transfer and os.path.exists(received)
                                  and sha256(received) == hashes[size])
                        row = {
                            "protocol": proto,
                            "loss_rate": f"{loss:.0%}",
                            "file_size": size,
                            "run": run,
                            "completed": ok_transfer,
                            "file_intact": intact,
                            "packets": field(r"Packets:\s+(\d+)", out, int),
                            "timeouts": field(r"Timeouts:\s+(\d+)", out, int),
                            "retransmissions": field(r"Retransmissions:\s+(\d+)", out, int),
                            "dropped": field(r"Dropped (\d+)", stats_line, int),
                            "duplicates": field(r"duplicates (\d+)", stats_line, int),
                            "out_of_order": field(r"out of order (\d+)", stats_line, int),
                            "time_s": field(r"Transfer time:\s+([\d.]+)", out),
                            "throughput_Bps": field(r"Throughput:\s+([\d.]+)", out),
                        }
                        rows.append(row)
                        status = "PASS" if intact else "FAIL"
                        print(f"[{status}] {proto:8} loss {loss:>4.0%}  {size:>6}  run {run}  "
                              f"drops {row['dropped']}  retx {row['retransmissions']}  "
                              f"{row['time_s']} s", flush=True)
                        if not ok_transfer:
                            print("    sender output:", out.strip().splitlines()[-1:])
            finally:
                recv.stop()
    return rows


def write_results(rows, results_dir):
    with open(os.path.join(results_dir, "results.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    groups = defaultdict(list)
    for r in rows:
        groups[(r["protocol"], r["loss_rate"], r["file_size"])].append(r)

    def avg(items, key):
        vals = [r[key] for r in items if r[key] is not None]
        return sum(vals) / len(vals) if vals else float("nan")

    lines = ["# Part B reliability results", "",
             "| Protocol | Loss | Size | Intact | Avg drops | Avg dup | Avg out-of-order "
             "| Avg timeouts | Avg retx | Avg time (s) |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for (proto, loss, size), items in groups.items():
        intact = sum(r["file_intact"] for r in items)
        lines.append(
            f"| {proto} | {loss} | {size} | {intact}/{len(items)} "
            f"| {avg(items, 'dropped'):.1f} | {avg(items, 'duplicates'):.1f} "
            f"| {avg(items, 'out_of_order'):.1f} | {avg(items, 'timeouts'):.1f} "
            f"| {avg(items, 'retransmissions'):.1f} | {avg(items, 'time_s'):.3f} |")
    total_ok = sum(r["file_intact"] for r in rows)
    lines += ["", f"**{total_ok}/{len(rows)} transfers produced an identical file.**"]
    with open(os.path.join(results_dir, "summary.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines[-1:]))


def duplicate_checks(args, results_dir):
    """Send a scripted sequence with a duplicate and an out-of-order packet
    straight to each receiver (0% loss) and check its ACKs and output file."""
    import socket
    from rdt_common import DATA, END, START, make_packet, parse_packet

    chunks = [b"A" * 100, b"B" * 100, b"C" * 100]
    script = [  # (description, packet)
        ("START seq 0", make_packet(START, 0, payload=b"dup_test.bin|300")),
        ("DATA seq 1", make_packet(DATA, 1, payload=chunks[0])),
        ("DATA seq 1 again (duplicate)", make_packet(DATA, 1, payload=chunks[0])),
        ("DATA seq 3 (out of order)", make_packet(DATA, 3, payload=chunks[2])),
        ("DATA seq 2", make_packet(DATA, 2, payload=chunks[1])),
        ("DATA seq 3", make_packet(DATA, 3, payload=chunks[2])),
        ("END seq 4", make_packet(END, 4)),
    ]
    expected_acks = {
        "stopwait": [0, 1, 1, None, 2, 3, 4],   # out of order: ignored, no ACK
        "gbn":      [0, 1, 1, 1, 2, 3, 4],      # out of order: re-ACK last in-order
    }
    lines = ["# Duplicate / out-of-order handling", ""]
    all_ok = True
    for proto, (_, receiver) in PROTOCOLS.items():
        out_dir = os.path.join(results_dir, f"dupcheck_{proto}")
        recv = Receiver(receiver, args.port, 0.0, out_dir, args.seed,
                        os.path.join(results_dir, f"dupcheck_{proto}.log"))
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(0.3)
        lines += [f"## {proto}", "", "| Packet sent | ACK expected | ACK received | OK |",
                  "|---|---|---|---|"]
        try:
            for (desc, raw), want in zip(script, expected_acks[proto]):
                sock.sendto(raw, ("127.0.0.1", args.port))
                try:
                    got = parse_packet(sock.recvfrom(2048)[0]).ack
                except socket.timeout:
                    got = None
                ok = got == want
                all_ok &= ok
                lines.append(f"| {desc} | {want if want is not None else 'none'} "
                             f"| {got if got is not None else 'none'} | {'yes' if ok else 'NO'} |")
            recv.wait_for("Dropped", 5)
        finally:
            sock.close()
            recv.stop()
        path = os.path.join(out_dir, "dup_test.bin")
        content_ok = os.path.exists(path) and open(path, "rb").read() == b"".join(chunks)
        all_ok &= content_ok
        lines += ["", f"File written exactly once, in order (AAA..BBB..CCC, 300 bytes): "
                      f"**{'yes' if content_ok else 'NO'}**", ""]
    with open(os.path.join(results_dir, "duplicate_check.md"), "w") as f:
        f.write("\n".join(lines))
    print(f"Duplicate/out-of-order check: {'PASS' if all_ok else 'FAIL'}")


def make_traces(args, results_dir):
    """Small verbose transfer at 30% loss for each protocol, for the report."""
    trace_dir = os.path.join(results_dir, "traces")
    os.makedirs(trace_dir, exist_ok=True)
    path = make_test_file(os.path.join(results_dir, "files"), "8KB")
    for proto, (sender, receiver) in PROTOCOLS.items():
        recv = Receiver(receiver, args.port, 0.3, os.path.join(trace_dir, f"received_{proto}"),
                        args.seed, os.path.join(trace_dir, f"{proto}_receiver.log"),
                        verbose=True)
        try:
            run_sender(sender, path, args.port, args.timeout, 4,
                       os.path.join(trace_dir, f"{proto}_sender.log"), verbose=True)
            recv.wait_for("Dropped", 10)
        finally:
            recv.stop()
    print(f"Traces written to {trace_dir}")


def main():
    parser = argparse.ArgumentParser(description="Part B reliability tests")
    parser.add_argument("--sizes", default="10KB,100KB,1MB",
                        help="comma-separated file sizes")
    parser.add_argument("--losses", default="0,0.1,0.2,0.3",
                        help="comma-separated loss rates")
    parser.add_argument("--runs", type=int, default=3, help="runs per size per loss rate")
    parser.add_argument("--timeout", type=float, default=0.1, help="sender timeout (s)")
    parser.add_argument("--window", type=int, default=8, help="GBN window size")
    parser.add_argument("--port", type=int, default=12399)
    parser.add_argument("--seed", type=int, default=372, help="receiver loss seed")
    parser.add_argument("--results", default="reliability_results")
    parser.add_argument("--no-trace", action="store_true", help="skip verbose traces")
    args = parser.parse_args()

    results_dir = os.path.abspath(args.results)
    os.makedirs(results_dir, exist_ok=True)
    duplicate_checks(args, results_dir)
    rows = run_tests(args, results_dir)
    write_results(rows, results_dir)
    if not args.no_trace:
        make_traces(args, results_dir)


if __name__ == "__main__":
    main()
