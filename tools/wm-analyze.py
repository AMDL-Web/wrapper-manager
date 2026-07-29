#!/usr/bin/env python3
"""Turn wrapper-manager's log into answers. Reads the log on stdin.

Kept out of the shell script on purpose: the analysis needs regexes full of
quotes and braces, and nesting that inside `python3 -c '...'` inside a shell
function inside an ssh heredoc is how you get a syntax error at 2am instead of
a diagnosis.

Sections:
  instances  per-instance verdict — the one that answers "is it actually OK?"
  wedge      the key-setup lock: who is holding one it will never release

The old `cliff` section is gone. It reported a concurrency cliff that does not
exist, and it was wrong twice over:

  * It derived each key setup's interval as [end - duration, end], so a 29.5 s
    sample owned a 29.5 s window and mechanically overlapped its neighbours,
    while a 0.4 s sample could overlap almost nothing. Overlap was a restatement
    of duration. The metric measured the same thing on both axes and drew a
    correlation between them.
  * It pooled both instances, so one wedged wrapper supplied nearly every sample
    in the high-overlap buckets and the curve read as a property of load.

What it was really binning: those slow samples decrypted *nothing*. Across
2026-07-29/30, 201 first-after-switch events under 15 s produced 465,164 steady
samples between them, and all 42 events above 15 s produced zero. They are
time-to-failure, not slow successes, and they only appeared as latency because
the manager recorded them before checking the error. It now logs them under
`first-after-switch-failed`, which this file keeps apart.

Proved by experiment afterwards: ten single-track jobs submitted strictly
sequentially, one key setup in the system at a time, still failed 4 of 10 — a
higher rate than a 204-track job. Concurrency was never the variable.
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

# The wrapper prints this from inside the lock that serialises its key setup, so
# its absence is the wedge and its presence is proof the lock still moves.
ANNOUNCE = "[.] adamId:"
EXCEPTION = "[!] catched an exception"


def seconds(value: str, unit: str) -> float:
    v = float(value)
    return v / 1e6 if unit == "µs" else v / 1e3 if unit == "ms" else v


def parse(lines):
    """One pass; both sections read from it."""
    inst = {}
    for line in lines:
        who = WHO.search(line) or INST.search(line)
        if not who:
            continue
        d = inst.setdefault(
            who.group(1),
            {"ready": 0, "died": 0, "gaveup": False, "devtok": 0,
             "tracks": 0, "samples": 0, "key": [], "stalled": [],
             "last_death": "?", "announces": 0, "last_announce": None,
             "threw": None, "threw_text": "", "announce_after_throw": 0,
             "condemned": 0},
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
        if "is unhealthy:" in line:
            d["condemned"] += 1
        # The lock markers. Order matters: an announce after a throw means the
        # wrapper took the lock again, so nothing leaked.
        if ANNOUNCE in line:
            d["announces"] += 1
            d["last_announce"] = stamp
            if d["threw"]:
                d["announce_after_throw"] += 1
        if EXCEPTION in line:
            if not d["threw"]:
                d["threw"] = stamp
                # From the marker, not the tail of the line: logrus wraps the
                # whole thing in msg="..." and the tail is the closing quote.
                d["threw_text"] = line[line.find(EXCEPTION):].strip().rstrip('"').strip()
            d["announce_after_throw"] = 0
        if "kind=steady " in line:
            d["tracks"] += 1
            n = N.search(line)
            if n:
                d["samples"] += int(n.group(1))
        # Successes and failures are separate populations in the log now, and
        # conflating them is what produced the phantom cliff. `key` is what the
        # wrapper actually costs; `stalled` is how long we waited for nothing.
        if "kind=first-after-switch " in line:
            m = MEAN.search(line)
            if m:
                d["key"].append(seconds(*m.groups()))
        if "kind=first-after-switch-failed" in line:
            m, n = MEAN.search(line), N.search(line)
            if m:
                d["stalled"].extend([seconds(*m.groups())] * (int(n.group(1)) if n else 1))
    return inst


def wedged(d):
    """Is this instance holding a key-setup lock it will never release?

    The wrapper takes one global mutex for all key setup and does not release it
    when its key-setup path throws — there is no landing pad on that route. So a
    single exception strands every later key setup forever, while the process
    keeps accepting connections and keeps reporting itself ready.

    The signature is exact: it threw, and it has not printed a single announce
    since. The announce is emitted from inside that lock, so one appearing after
    the throw proves the lock still moves.
    """
    return bool(d["threw"]) and d["announce_after_throw"] == 0


def verdict(d):
    """The judgement. 'Ready' is not in it, deliberately.

    On 2026-07-29 the broken instance reached Ready every single time — it got
    a token, it started listening, and it could not decrypt. Readiness proved
    nothing.

    The order of these branches is itself a bug fix. WORKING/STRAINED used to be
    keyed on `kind=steady` lines, and a wedged instance never reaches steady
    state, so it fell all the way through to "ready, but has decrypted nothing"
    — UNPROVEN, phrased as though the window were merely quiet, while that
    instance was failing a third of every track submitted. The one case this
    tool exists to catch was the one case it could not name. So the wedge is
    tested first, before any branch that needs successful work to exist.
    """
    if d["gaveup"]:
        return "FAILED", "supervisor gave up restarting it — needs attention"
    if wedged(d):
        return "WEDGED", (f"threw at {d['threw']} and has not taken the key-setup "
                          f"lock since ({d['threw_text']}) — every key setup on it "
                          "now blocks forever; only a restart clears it")
    if d["devtok"]:
        return "BROKEN", (f"{d['devtok']}x devToken error — this device cannot get "
                          "a token from Apple; it needs a fresh login")
    if d["stalled"] and not d["tracks"]:
        return "STALLING", (f"{len(d['stalled'])} first samples returned nothing and "
                            "no track completed — it is not slow, it is not answering")
    if d["tracks"]:
        med = st.median(d["key"]) if d["key"] else 0.0
        stalls = len(d["stalled"])
        if stalls:
            return "FAULTING", (f"decrypted {d['tracks']} tracks, but {stalls} key "
                                "setups returned nothing at all — check for a throw")
        if med >= 3:
            return "SLOW", (f"working, every key setup completing, median {med:.1f}s. "
                            "Not the wedge; look at host load")
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
                  f"max={k[-1]:.2f}s  (completed)")
        if d["stalled"]:
            s = sorted(d["stalled"])
            print(f"{'':14}STALLED   : n={len(s)} median={st.median(s):.2f}s "
                  f"max={s[-1]:.2f}s  (returned nothing — time-to-failure, not latency)")
        print(f"{'':14}lock      : {d['announces']} announces"
              + (f", last {d['last_announce']}" if d["last_announce"] else "")
              + (f"; threw at {d['threw']}" if d["threw"] else ""))
        if d["condemned"]:
            print(f"{'':14}condemned : {d['condemned']}x by the manager")
        if d["died"]:
            print(f"{'':14}deaths    : {d['died']}, last at {d['last_death']}")
    print()


def wedge(inst):
    """The section that replaces `cliff`.

    It answers the question the cliff table was reaching for and getting wrong:
    when key setup does not complete, why. There is only one known cause, it has
    an exact signature, and it is not load.
    """
    print("── key-setup lock ─────────────────────────────────────")
    if not inst:
        print("   nothing in this window — widen it, or the manager is idle\n")
        return
    any_stall = False
    for name, d in sorted(inst.items()):
        done, stalled = len(d["key"]), len(d["stalled"])
        if not (done or stalled or d["threw"]):
            continue
        any_stall = any_stall or bool(stalled)
        share = f"{stalled}/{done + stalled}" if done + stalled else "0/0"
        state = "WEDGED" if wedged(d) else ("faulting" if stalled else "ok")
        print(f"   {name}  {state:<8} key setups stalled {share}"
              f"   announces={d['announces']}")
        if d["threw"]:
            print(f"{'':14}threw at {d['threw']}: {d['threw_text']}")
            print(f"{'':14}announces after that throw: {d['announce_after_throw']}"
                  + ("   <-- none, the lock is held forever"
                     if d["announce_after_throw"] == 0 else ""))
    if not any_stall and not any(wedged(d) for d in inst.values()):
        print("   every key setup in this window completed.")
    elif not any_stall:
        print("   NOTE: no stalled setups are broken out above. A log written by a")
        print("   manager older than the first-after-switch-failed split reports")
        print("   time-to-failure as latency, so read the 29-30s figures as failures.")
    print()
    print("   A stalled key setup is not a slow one. The wrapper takes one global")
    print("   mutex for all key setup and leaks it if its key-setup path throws,")
    print("   so one exception strands every later setup on that process while it")
    print("   still reports Ready. The manager now condemns on this directly.")
    print("   Concurrency is NOT the lever: ten strictly sequential single-track")
    print("   jobs still failed 4 of 10, and raising or lowering parallelism has")
    print("   never moved it. Do not reach for max_parallel_decrypts.")
    print()


def main():
    want = sys.argv[1] if len(sys.argv) > 1 else "all"
    inst = parse(sys.stdin)
    if want in ("all", "instances"):
        instances(inst)
    if want in ("all", "wedge", "cliff"):
        if want == "cliff":
            print("   (`cliff` measured an artifact and is now `wedge`; see the")
            print("    module docstring in wm-analyze.py for why.)\n")
        wedge(inst)


if __name__ == "__main__":
    main()
