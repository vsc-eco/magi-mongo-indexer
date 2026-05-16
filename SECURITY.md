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

### Required operator action if you re-enable public access

Hasura metadata (table/column permissions) is **not** version-controlled in
this repo — it is configured at runtime. If you set
`HASURA_GRAPHQL_UNAUTHORIZED_ROLE=public` you MUST, in Hasura metadata,
restrict the `public` role to non-sensitive tables/columns only. Do **not**
grant `public` select on tables containing deposit addresses, withdrawal
destinations, raw balances, or TSS/key material. Keep the admin secret
strong and never expose the console publicly in production.
