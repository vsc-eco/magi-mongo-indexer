# Security notes

## CRITICAL #7 — Hasura GraphQL exposure (review2)

Previously `docker-compose.yaml` / `docker-compose.testnet.yaml` hardcoded:

```
HASURA_GRAPHQL_ENABLE_CONSOLE: "true"
HASURA_GRAPHQL_DEV_MODE: "true"
HASURA_GRAPHQL_UNAUTHORIZED_ROLE: public
```

This meant any unauthenticated client received the `public` role and could
read every table the role was granted in Hasura metadata — including BTC
balances, deposit addresses and withdrawal destinations — and the admin
console + verbose dev errors + introspection were served openly.

### What changed

All three are now env-driven with **secure defaults**:

| Env var | Default (unset) | Effect |
|---------|-----------------|--------|
| `HASURA_GRAPHQL_ENABLE_CONSOLE` | `false` | Console (`/console`) is off |
| `HASURA_GRAPHQL_DEV_MODE` | `false` | No verbose errors / introspection leakage |
| `HASURA_GRAPHQL_UNAUTHORIZED_ROLE` | *(empty)* | Anonymous requests are **rejected** |

Existing deployments that legitimately serve public read data are **not
silently broken in a hidden way** — they must explicitly opt back in by
setting the variables in their `.env`, which forces a conscious decision.

### Upgrade runbook for existing operators

After pulling this change, an existing deployment that serves a public
explorer will reject anonymous GraphQL until the operator opts back in:

1. `git pull` (the new compose files)
2. Edit `.env` and add **only**:
   ```
   HASURA_GRAPHQL_UNAUTHORIZED_ROLE=public
   ```
   Do **not** re-add `HASURA_GRAPHQL_ENABLE_CONSOLE=true` or
   `HASURA_GRAPHQL_DEV_MODE=true` — the explorer does not need them and they
   are the genuinely unsafe surface (public admin console, introspection,
   verbose errors).
3. `docker compose up -d`
   **Not** `docker compose restart` — `restart` reuses the existing
   container config and will not pick up the changed `.env`/compose. `up -d`
   recreates the `hasura` container with the new environment.

Operators who do not need anonymous reads do nothing — secure-by-default
applies automatically.

### Required operator action if you re-enable public access

Hasura metadata (table/column permissions) is **not** version-controlled in
this repo — it is configured at runtime. If you set
`HASURA_GRAPHQL_UNAUTHORIZED_ROLE=public` you MUST, in Hasura metadata,
restrict the `public` role to non-sensitive tables/columns only. Do **not**
grant `public` select on tables containing deposit addresses, withdrawal
destinations, raw balances, or TSS/key material. Keep the admin secret
strong and never expose the console publicly in production.
