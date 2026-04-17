---
name: systematic-debugging
description: 系统化调试 skill。出现难复现 bug、偶现问题、线上报错、性能异常、依赖冲突、环境差异导致的故障时使用。目标是用可验证的假设与最小实验快速定位根因并修复。
---

# Systematic Debugging

## Workflow

1. Define the symptom precisely (what/where/when), and collect reproduction steps
2. Establish a baseline:
   - expected behavior vs actual behavior
   - smallest failing input / minimal repro
3. Generate hypotheses and rank by likelihood + test cost
4. Run one experiment per hypothesis; record observations
5. Isolate root cause and apply the smallest safe fix
6. Add regression test (or a verification script) if the repo supports it

## Techniques

- Binary search the change surface (feature flags, config toggles, commits)
- Add temporary instrumentation locally (avoid logging secrets)
- Reduce variables: pin versions, disable concurrency, mock external services
- Check invariants at boundaries: API inputs, DB writes, file IO

## Output

- Root cause explanation (1–3 bullets)
- Fix summary
- How to verify locally and prevent regressions
