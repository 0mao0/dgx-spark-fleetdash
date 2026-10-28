#!/usr/bin/env python3
"""
VRAM / unified-memory guardian for DGX Spark (GB10).

BACKGROUND
  GB10 has NO discrete VRAM: GPU and CPU share one ~128 GB physical pool.
  The vLLM services (llm/embed/rerank) are hard-capped by --gpu-memory-utilization,
  but the torch services (tts/asr/face) are UNCAPPED. When all services are hit
  at once the pool can be exhausted -> `NVRM Out of memory` -> hard hang
  (incident 2026-09-01 02:22). Swap (32 GB file) does NOT help, because GPU
  memory cannot be paged to disk.

  This guardian watches committed physical memory (MemTotal - MemAvailable) and
  SHEDS the uncapped torch services BEFORE the pool is exhausted, then RESTORES
  them when memory recovers. It NEVER touches the capped vLLM services.

STATE MACHINE  (all thresholds in GiB of committed physical memory)
  NORMAL    -> >= WARN   sustained  -> shed SHED_DEGRADED units          -> DEGRADED
  DEGRADED  -> >= CRIT   sustained  -> shed SHED_CRITICAL units         -> CRITICAL
  DEGRADED  -> < RECOVER sustained  -> restore DEGRADED units           -> NORMAL
  CRITICAL  -> < RECOVER sustained  -> restore all                      -> NORMAL

TUNING (env)
  VRAM_GUARDIAN_WARN / _CRIT / _RECOVER   GiB thresholds
  VRAM_GUARDIAN_SAMPLE                     poll seconds (default 5)
  VRAM_GUARDIAN_SUSTAIN                    seconds above threshold before shedding (default 20)
  VRAM_GUARDIAN_RECOVER_SEC                seconds below recover before restoring (default 180)
  VRAM_GUARDIAN_SHED_DEGRADED              comma-separated units for WARN tier (default none)
  VRAM_GUARDIAN_SHED_CRITICAL              comma-separated units for CRIT tier (default face-api)
  VRAM_GUARDIAN_DRYRUN=1                   log only, no systemctl actions (safe observation)

Only the uncapped services are ever stopped. The capped vLLM engines are sacred.
"""
import os
import time
import subprocess
import datetime

LOG_FILE = os.environ.get("VRAM_GUARDIAN_LOG",
                          os.path.expanduser("~/logs/vram_guardian.log"))
MEM_WARN_GIB    = float(os.environ.get("VRAM_GUARDIAN_WARN", "108"))
MEM_CRIT_GIB    = float(os.environ.get("VRAM_GUARDIAN_CRIT", "115"))
MEM_RECOVER_GIB = float(os.environ.get("VRAM_GUARDIAN_RECOVER", "90"))
SAMPLE_SEC      = int(os.environ.get("VRAM_GUARDIAN_SAMPLE", "5"))
SUSTAIN_SEC     = int(os.environ.get("VRAM_GUARDIAN_SUSTAIN", "20"))
RECOVER_SEC     = int(os.environ.get("VRAM_GUARDIAN_RECOVER_SEC", "180"))
DRY_RUN         = os.environ.get("VRAM_GUARDIAN_DRYRUN", "0") == "1"


def _env_units(name, default):
    """逗号分隔的 shed 名单 env；未设置时用内置默认。"""
    raw = os.environ.get(name, "").strip()
    if raw:
        return [u.strip() for u in raw.split(",") if u.strip()]
    return list(default)


# Uncapped services we may shed. Capped vLLM engines are NEVER in this list.
SHED_DEGRADED = _env_units("VRAM_GUARDIAN_SHED_DEGRADED", [])
SHED_CRITICAL = _env_units("VRAM_GUARDIAN_SHED_CRITICAL", ["face-api"])

HEARTBEAT_EVERY = max(1, int(60 / SAMPLE_SEC))  # log a heartbeat ~every 60s


def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = "[%s] %s" % (ts, msg)
    print(line, flush=True)
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def committed_gib():
    mi = {}
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                k, _, v = line.partition(":")
                v = v.strip().split()
                if v:
                    try:
                        mi[k.strip()] = int(v[0])   # KiB
                    except ValueError:
                        pass
    except Exception:
        return None
    total = mi.get("MemTotal", 0)
    avail = mi.get("MemAvailable", 0)
    if not total:
        return None
    # MiB -> GiB
    return (total - avail) / 1048576.0


def gpu_sum_mib():
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-compute-apps=used_memory",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10).stdout
        s = 0
        for ln in out.strip().splitlines():
            ln = ln.strip()
            if ln:
                s += int(float(ln))
        return s
    except Exception:
        return None


def is_active(unit):
    try:
        r = subprocess.run(["systemctl", "is-active", unit],
                           capture_output=True, text=True, timeout=10)
        return r.stdout.strip() == "active"
    except Exception:
        return False


def act(unit, start):
    verb = "start" if start else "stop"
    if DRY_RUN:
        log("[DRYRUN] would %s %s" % (verb, unit))
        return
    try:
        subprocess.run(["systemctl", verb, unit], timeout=30, check=True)
        log("%s %s OK" % (verb, unit))
    except Exception as e:
        log("%s %s FAILED: %s" % (verb, unit, e))


def main():
    log("guardian started dry_run=%s warn=%s crit=%s recover=%s "
        "shed_degraded=%s shed_critical=%s"
        % (DRY_RUN, MEM_WARN_GIB, MEM_CRIT_GIB, MEM_RECOVER_GIB,
           SHED_DEGRADED, SHED_CRITICAL))

    state = "NORMAL"
    shed = set()
    # Reconcile: if a sheddable service is already down at startup, remember to
    # restore it later and assume we are already in a shed state.
    for u in SHED_DEGRADED + SHED_CRITICAL:
        if not is_active(u):
            shed.add(u)
    if shed & set(SHED_CRITICAL):
        state = "CRITICAL"
    elif shed & set(SHED_DEGRADED):
        state = "DEGRADED"
    if shed:
        log("startup reconcile: already stopped=%s state=%s" % (sorted(shed), state))

    above_since = None
    below_since = None
    samples = 0

    while True:
        c = committed_gib()
        g = gpu_sum_mib()
        samples += 1
        if c is None:
            time.sleep(SAMPLE_SEC)
            continue
        gstr = ("gpu=%.1fGiB" % (g / 1024.0)) if g is not None else "gpu=?"
        if samples % HEARTBEAT_EVERY == 0 or DRY_RUN:
            log("committed=%.1fGiB %s state=%s shed=%s"
                % (c, gstr, state, sorted(shed)))

        now = time.time()
        if state == "NORMAL":
            if c >= MEM_WARN_GIB:
                if above_since is None:
                    above_since = now
                if now - above_since >= SUSTAIN_SEC:
                    for u in SHED_DEGRADED:
                        if is_active(u):
                            act(u, False)
                            shed.add(u)
                    log("ENTER DEGRADED (committed=%.1f >= warn=%s)" % (c, MEM_WARN_GIB))
                    state = "DEGRADED"
                    above_since = None
                    below_since = None
            else:
                above_since = None
        elif state == "DEGRADED":
            if c >= MEM_CRIT_GIB:
                if above_since is None:
                    above_since = now
                if now - above_since >= SUSTAIN_SEC:
                    for u in SHED_CRITICAL:
                        if is_active(u):
                            act(u, False)
                            shed.add(u)
                    log("ENTER CRITICAL (committed=%.1f >= crit=%s)" % (c, MEM_CRIT_GIB))
                    state = "CRITICAL"
                    above_since = None
                    below_since = None
            elif c < MEM_RECOVER_GIB:
                if below_since is None:
                    below_since = now
                if now - below_since >= RECOVER_SEC:
                    for u in SHED_DEGRADED:
                        if u in shed:
                            act(u, True)
                            shed.discard(u)
                    log("RECOVER -> NORMAL (services restored)")
                    state = "NORMAL"
                    above_since = None
                    below_since = None
            else:
                below_since = None
        elif state == "CRITICAL":
            if c < MEM_RECOVER_GIB:
                if below_since is None:
                    below_since = now
                if now - below_since >= RECOVER_SEC:
                    for u in SHED_CRITICAL + SHED_DEGRADED:
                        if u in shed:
                            act(u, True)
                            shed.discard(u)
                    log("RECOVER -> NORMAL (all restored)")
                    state = "NORMAL"
                    above_since = None
                    below_since = None
            else:
                below_since = None
        time.sleep(SAMPLE_SEC)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("guardian stopped by signal")
