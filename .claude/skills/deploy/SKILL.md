---
name: deploy
description: Use when deploying services to staging — Cloud Run (dealsync-service via gcloud builds submit), W3 workflows (deploy via MCP), and W3 actions (dealsync-w3-actions via npm + git push).
allowed-tools: Bash, Read
---

# Deploy to Staging

## Services

### dealsync-service

```bash
gcloud builds submit \
  --config=dealsync-service/cloudbuild-staging.yaml \
  --project=creatorland-backend-staging \
  --substitutions=COMMIT_SHA=$(git rev-parse HEAD) .
```

**Cloud Run service:** `dealsync-v2-service`
**Region:** `us-central1`
**Project:** `creatorland-backend-staging`
**Image registry:** `us-central1-docker.pkg.dev/creatorland-backend-staging/dealsync-v2-images/dealsync-service`

### W3 Workflows

Deploy to testnet at `https://1.w3-testnet.io`:

```
mcp__w3__deploy-workflow with url: https://1.w3-testnet.io
```

W3 environment: `0x226c2acd33ef649bff3339670b6de489880a094acf7390df6cad4b5e18a17665`

**Workflow naming convention:** Use commit hash like Docker images:

- `dealsync-orchestrator-{commit}` (e.g., `dealsync-orchestrator-8ad60b5`)
- `dealsync-processor-{commit}` (e.g., `dealsync-processor-8ad60b5`)
- NO version numbers (v1, v2, etc.)
- The orchestrator's `processor-name` input must match the deployed processor name

### Actions (dealsync-w3-actions)

```bash
cd /tmp/dealsync-w3-actions-v2
npm test && npm run package
git add -A && git commit && git push origin main
# Use the commit hash in workflow YAML: @<commit-hash>
```

## Key Notes

- W3 workflows can't be overwritten — must use versioned names (e.g., `Dealsync Processor v24`)
- W3 runtime doesn't support `.*` glob expressions in YAML — use `fetch-content` command instead
- W3 runtime uses `console.log()` for logs, NOT `core.info()`
- `w3-sxt-action` only works for DQL queries — use `dealsync` action's `sxt-execute` for DML
- SxT doesn't support multi-statement or multi-row INSERT — each INSERT is a separate API call
- `SXT_BISCUIT` in W3 env must be a multi-table biscuit covering: deal_states, ai_evaluation_audits, email_thread_evaluations, deals, deal_contacts, contacts
