# W3 Portable Action Patterns

## Overview

Portable actions run identically on both W3 and GitHub Actions. The reference implementation is `w3-io/demo-gha-containerization-1`. Follow these patterns when building new JavaScript actions for the DealSync workflow migration.

## Reference Repository

**Repo:** `w3-io/demo-gha-containerization-1`

```bash
gh repo view w3-io/demo-gha-containerization-1
gh api repos/w3-io/demo-gha-containerization-1/git/trees/main?recursive=1 --jq '.tree[].path'
```

## Action Directory Structure

```
repo-root/
  my-action/
    action.yml          # GHA action metadata (required)
    dist/index.js       # Bundled action (committed to repo)
  src/
    my-action/
      index.js          # Entrypoint (imports main.js)
      main.js           # Core logic
    crypto.js           # Shared modules
  scripts/
    decrypt.js          # CLI helpers for shell steps
  rollup.config.js      # Bundles src/ -> action/dist/
  package.json          # "type": "module", node >= 20
```

**Key principle:** Source in `src/`, bundled output in `<action>/dist/index.js`, both committed. CI verifies dist matches source.

## action.yml Format

```yaml
name: 'My Action'
description: 'What it does'
inputs:
  url:
    description: 'Target URL'
    required: true
  method:
    description: 'HTTP method'
    required: false
    default: 'GET'
  encryption-key:
    description: 'AES-256-GCM key for encrypt/decrypt'
    required: false
outputs:
  response:
    description: 'The response body'
  success:
    description: 'Whether the request succeeded'
runs:
  using: node24
  main: dist/index.js
```

**Runtime:** Use `node24` (latest supported). Actions run as standard GHA JavaScript actions on both platforms.

## Rollup Bundling

```javascript
// rollup.config.js pattern
export default [
  {
    input: 'src/my-action/index.js',
    output: { file: 'my-action/dist/index.js', format: 'esm' },
    // Bundle @actions/core, @actions/io etc. into dist
  },
]
```

Build and verify:

```bash
npm run build         # Rollup bundles src/ -> dist/
npm run check-dist    # Verifies dist/ matches source
```

## Portability Rules

1. **Only `workflow_dispatch` trigger** — the only trigger portable across W3 and GHA
2. **`runs-on: ubuntu-latest`** — available on both platforms
3. **`W3_SECRET_` prefix on secrets** — `secrets.W3_SECRET_MY_KEY` works on both (GHA reads from repo secrets, W3 from its secret store)
4. **No `actions/checkout`** — W3 has no repo context; reference actions via `uses: owner/repo/path@ref`
5. **No `${{ github.* }}` context** — not available on W3; use `${{ inputs.* }}` instead
6. **ES Modules throughout** — `"type": "module"` in package.json
7. **Self-contained actions** — each action bundles all deps into `dist/index.js`

## Encryption Pattern

All sensitive data flowing between steps/jobs uses AES-256-GCM encryption:

```yaml
steps:
  - uses: w3-io/demo-gha-containerization-1/http@main
    with:
      url: https://api.example.com/auth
      method: POST
      body: '{"credentials": "..."}'
      encrypt-outputs: 'sessionId,accessToken' # Encrypt specific output fields
      encryption-key: ${{ secrets.W3_SECRET_ENCRYPTION_KEY }}

  - uses: w3-io/demo-gha-containerization-1/http@main
    with:
      url: https://api.example.com/data
      headers: '{"Authorization": "${{ steps.auth.outputs.accessToken }}"}'
      decrypt-inputs: 'headers.Authorization' # Decrypt before use
      encryption-key: ${{ secrets.W3_SECRET_ENCRYPTION_KEY }}
```

**Why encryption matters:** Workflow logs may be visible. Encrypting sensitive data in transit between steps prevents credential leakage regardless of platform.

## The `http` Action Reference

The core reusable action for making HTTP requests with built-in crypto:

| Input             | Required | Description                                                               |
| ----------------- | -------- | ------------------------------------------------------------------------- |
| `url`             | Yes      | Target URL                                                                |
| `method`          | No       | HTTP method (default: GET)                                                |
| `headers`         | No       | JSON string of headers                                                    |
| `body`            | No       | Request body                                                              |
| `decrypt-inputs`  | No       | Dot-notation paths to decrypt (e.g., `headers.sid,headers.Authorization`) |
| `extract-outputs` | No       | Fields to extract from JSON response as individual outputs                |
| `encrypt-outputs` | No       | Output fields to encrypt                                                  |
| `encryption-key`  | No       | AES-256-GCM key                                                           |
| `select-keys`     | No       | Filter response object keys                                               |
| `limit`           | No       | Limit array response length                                               |

| Output        | Description                  |
| ------------- | ---------------------------- |
| `success`     | Whether request succeeded    |
| `status-code` | HTTP status code             |
| `response`    | Response body (or encrypted) |

## Workflow Pattern: Multi-Job Pipeline

```yaml
name: 'DealSync Pipeline'
on: workflow_dispatch

jobs:
  fetch-data:
    runs-on: ubuntu-latest
    outputs:
      result: ${{ steps.fetch.outputs.response }}
    steps:
      - id: fetch
        uses: w3-io/demo-gha-containerization-1/http@main
        with:
          url: https://api.spaceandtime.io/v2/sql
          method: POST
          body: '{"sql": "SELECT ..."}'
          encrypt-outputs: 'response'
          encryption-key: ${{ secrets.W3_SECRET_ENCRYPTION_KEY }}

  process:
    runs-on: ubuntu-latest
    needs: fetch-data
    steps:
      - id: classify
        uses: w3-io/demo-gha-containerization-1/http@main
        with:
          url: https://api.hyperbolic.xyz/v1/chat/completions
          method: POST
          body: '${{ needs.fetch-data.outputs.result }}'
          decrypt-inputs: 'body'
          encryption-key: ${{ secrets.W3_SECRET_ENCRYPTION_KEY }}
```

## Fallback Pattern for Unreliable Services

```yaml
- id: primary
  continue-on-error: true
  uses: w3-io/demo-gha-containerization-1/http@main
  with:
    url: https://api.example.com/v1/chat
    body: '{"model": "deepseek-v3", ...}'

- id: fallback
  if: steps.primary.outputs.success != 'true'
  uses: w3-io/demo-gha-containerization-1/http@main
  with:
    url: https://api.example.com/v1/chat
    body: '{"model": "qwen-2.5", ...}'
```

## Shell Steps with Encrypted Data

When `run:` steps need to work with encrypted data, use the CLI helper:

```yaml
- run: |
    DECRYPTED=$(node scripts/decrypt.js "$ENCRYPTED_VALUE" "$KEY")
    echo "result=$DECRYPTED" >> "$GITHUB_OUTPUT"
  env:
    ENCRYPTED_VALUE: ${{ steps.prev.outputs.encrypted_field }}
    KEY: ${{ secrets.W3_SECRET_ENCRYPTION_KEY }}
```

## Common Mistakes

| Mistake                                     | Fix                                                           |
| ------------------------------------------- | ------------------------------------------------------------- |
| Passing secrets in plain text between steps | Use `encrypt-outputs` / `decrypt-inputs` on the http action   |
| Using `actions/checkout`                    | Not available on W3; actions are fetched by `uses:` reference |
| Unbundled action (no dist/)                 | Actions must have bundled `dist/index.js` committed to repo   |
| Using `github.*` context                    | Use `inputs.*` or `secrets.*` instead                         |
| Shell state between steps                   | Each W3 step runs in separate container; inline everything    |
