---
name: w3
description: Use when working with W3 protocol workflows — deploying, debugging, building new actions, or understanding the legacy Rust pipeline being replaced. Covers workflow YAML authoring, portable action patterns, encryption, and the current Rust implementation reference.
allowed-tools: Read, Glob, Grep, Bash, WebFetch
paths: "src/commands/**,config/*.json"
---

# W3 Protocol Workflows

This skill covers everything related to W3 workflows for DealSync: building new actions, deploying workflows, and understanding the legacy system being replaced.

## When to Use

- **Building new W3 actions** → See [portable-actions.md](portable-actions.md)
- **Understanding the legacy Rust pipeline** → See [legacy-workflows.md](legacy-workflows.md)
- **Deploying workflows** → See the `deploy` skill instead
- **Querying SxT tables used by workflows** → See the `sxt` skill instead

## Quick Reference

### Portability Rules (actions that run on both W3 and GitHub Actions)

1. Only `workflow_dispatch` trigger
2. `runs-on: ubuntu-latest`
3. `W3_SECRET_` prefix on all secrets
4. No `actions/checkout` (W3 has no repo context)
5. No `${{ github.* }}` context — use `${{ inputs.* }}`
6. ES Modules throughout (`"type": "module"`)
7. Self-contained: each action bundles all deps into `dist/index.js`

### W3 Runtime Gotchas

- W3 runtime uses `console.log()` for logs, NOT `core.info()`
- W3 runtime doesn't support `.*` glob expressions in YAML
- YAML hex environment names (0x...) must be quoted to prevent YAML integer parsing
- Each W3 step runs in a separate container — no shell state between steps
- Workflow logs may be visible — encrypt sensitive data between steps

### Legacy Pipeline State Machine (stage-based, being replaced)

```
Stage 2 (Unprocessed) → 3 (Filtered) → 4 (Deal) / 106 (Rejected) / 107 (Non-English)
Transition stages: 10001-19999 (filter), 20001-59999 (detection) — distributed batch locks
```

### New Pipeline State Machine (status-based, current)

```
pending → filtering → filter_rejected (terminal)
       → pending_classification → classifying → deal / not_deal (terminal)
```
