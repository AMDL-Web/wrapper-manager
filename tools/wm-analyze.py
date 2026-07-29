#!/usr/bin/env python3
"""Turn wrapper-manager's log into answers. Reads the log on stdin.

Kept out of the shell script on purpose: the analysis needs regexes full of
quotes and braces, and nesting that inside `python3 -c '...'` inside a shell
function inside an ssh heredoc is how you get a syntax error at 2am instead of
a diagnosis.

Sections:
  instances  per-instance verdict — the one that answers "is it actually OK?"
  cliff      key-setup concurrency vs latency, the curve that collapses
"""
import sys
import re
import statistics as st
import datetime as dt

TIME = re.compile(r'time="([^"]+)"')
WHO = re.compile(r"wrapper ([0-9a-f]{8})")
INST = re.compile(r"instance=([0-9a-f]{8})")
MEAN = re.compile(r"mean=([\d.]+)(µs|ms|s)\b")
N = re.compile(r" n=(\d+)")


def seconds(value: str, unit: str) -> float:
    v = float(value)
    return v / 1e6 if unit == "µs" else v / 1e3 if unit == "ms" else v


def parse(lines):
    """One pass; both sections read from it."""
    inst, events = {}, []
    for line in lines:
        who = WHO.search(line) or INST.search(line)
        if not who:
            continue
        d = inst.setdefault(
            who.group(1),
            {"ready": 0, "died": 0, "gaveup": False, "devtok": 0,
             "tracks": 0, "samples": 0, "key": [], "last_death": "?"},
        )
        t = TIME.search(line)
        stamp = t.group(1)[11:19] if t else "?"

        if "Wrapper ready" in line:
            d["ready"] += 1
        if "wrapper exited" in line:
            d["died"] += 1
            d["last_death"] = stamp
        if "not restarting it again" in line:
            d["gaveup"] = True
        if "devToken error" in line:
            d["devtok"] += 1
        if "kind=steady" in line:
            d["tracks"] += 1
            n = N.search(line)
            if n:
                d["samples"] += int(n.group(1))
        if "kind=first-after-switch" in line:
            m = MEAN.search(line)
            if m and t:
                secs = seconds(*m.groups())
                d["key"].append(secs)
                end = dt.datetime.strptime(
                    t.group(1), "%Y-%m-%dT%H:%M:%SZ"
                ).timestamp()
                events.append((end - secs, end, secs, who.group(1)))
    return inst, events


def verdict(d):
    """The judgement. 'Ready' is not in it, deliberately.

    On 2026-07-29 the broken instance reached Ready every single time — it got
    a token, it started listening, and it could not decrypt. Readiness proved
    nothing. Recent successful work and the key-setup latency are the only two
    signals that separate a working instance from a dead one.
    """
    if d["gaveup"]:
        return "FAILED", "supervisor gave up restarting it — needs attention"
    if d["devtok"]:
        return "BROKEN", (f"{d['devtok']}x devToken error — this device cannot get "
                          "a token from Apple; it needs a fresh login")
    if d["tracks"]:
        med = st.median(d["key"]) if d["key"] else 0.0
        if med >= 3:
            return "STRAINED", (f"working, but key setup median {med:.1f}s — the cliff "
                                "starts here; lower download.max_parallel_decrypts")
        return "WORKING", f"decrypted {d['tracks']} tracks / {d['samples']} samples"
    if d["ready"]:
        return "UNPROVEN", ("ready, but has decrypted nothing in this window — "
                            "ready is not health, run a job to find out")
    return "SILENT", "no readiness and no work in this window"


def instances(inst):
    print("── per-instance verdict ───────────────────────────────")
    if not inst:
        print("   nothing in this window — widen it, or the manager is idle\n")
        return
    for name, d in sorted(inst.items()):
        v, why = verdict(d)
        print(f"   {name}  {v:<9} {why}")
        if d["key"]:
            k = sorted(d["key"])
            print(f"{'':14}key setup: n={len(k)} median={st.median(k):.2f}s "
                  f"max={k[-1]:.2f}s")
        if d["died"]:
            print(f"{'':14}deaths: {d['died']}, last at {d['last_death']}")
    print()


def cliff(events):
    print("── key-setup concurrency vs latency ───────────────────")
    if not events:
        print("   no samples in this window\n")
        return
    buckets = {}
    for start, end, dur, who in events:
        overlap = sum(
            1 for s2, e2, _, w2 in events
            if w2 == who and not (e2 <= start or s2 >= end)
        ) - 1
        buckets.setdefault(min(overlap, 12) // 2 * 2, []).append(dur)
    print(f"   {'overlap':>8} {'n':>5} {'median':>9} {'p90':>9}")
    for k in sorted(buckets):
        v = sorted(buckets[k])
        p90 = v[min(len(v) - 1, int(len(v) * 0.9))]
        print(f"   {k:>8} {len(v):>5} {st.median(v):>8.2f}s {p90:>8.2f}s")
    print()
    print("   Flat to ~6 is healthy; a jump past ~10 is the cliff. The lever is")
    print("   the backend's download.max_parallel_decrypts, NOT the manager's")
    print("   pool — CONCURRENCY_BENCHMARK.md measured raising the pool as worse.")
    print("   Budget roughly 10 parallel decrypts per healthy instance.")
    print()


def main():
    want = sys.argv[1] if len(sys.argv) > 1 else "all"
    inst, events = parse(sys.stdin)
    if want in ("all", "instances"):
        instances(inst)
    if want in ("all", "cliff"):
        cliff(events)


if __name__ == "__main__":
    main()
