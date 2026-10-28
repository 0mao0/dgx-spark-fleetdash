# components/guardian — GB10 统一内存保护

DGX Spark (GB10) 的 GPU/CPU 共享 ~128GB 统一内存池。vLLM 引擎有
`--gpu-memory-utilization` 硬顶，无顶的 torch 服务（tts/asr/face 类）叠加时
可能耗尽池子 → `NVRM Out of memory` → 整机硬死（swap 救不了 GPU 内存）。

本守护进程盯 committed memory（MemTotal − MemAvailable），在耗尽**前**按
`WARN → DEGRADED → CRITICAL` 三档逐级停掉无顶服务，内存回落后自动拉回。
有硬顶的 vLLM 引擎永不触碰。

```
NORMAL  ──持续≥WARN──>  DEGRADED（停 SHED_DEGRADED 名单）
DEGRADED ─持续≥CRIT──>  CRITICAL（停 SHED_CRITICAL 名单）
低于 RECOVER 持续 180s ──>  逐档拉回 NORMAL
```

安装（目标机上）：

```bash
sudo bash components/guardian/install.sh
```

- 脚本落 `~/scripts/vram_guardian.py`，unit 由模板按用户名生成。
- 阈值/shed 名单全部 env 化，调参写 override（`override.conf.example`）。
- 默认 CRITICAL 名单只有 `face-api`；`VRAM_GUARDIAN_SHED_CRITICAL=a,b` 改名单。
- `VRAM_GUARDIAN_DRYRUN=1` 只记日志不动服务，供观察。
- 日志 `~/logs/vram_guardian.log`。
