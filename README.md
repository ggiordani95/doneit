# AI Agent Swarm

Distributed multi-agent AI execution platform. LangGraph decides what happens next, Redpanda
moves the work, stateless workers execute agents, and GitHub and Jira are where the results
land: branches, pull requests and ticket transitions.

The full design is in [`AI_AGENT_SWARM_SPEC.md`](AI_AGENT_SWARM_SPEC.md).

## Status

| Phase | Scope | State |
|-------|-------|-------|
| 1 | Infrastructure: Redpanda, Console, PostgreSQL, Redis, uv workspace, topic bootstrap | done |
| 2 | Messaging: `MessageBus`, Redpanda adapter, Pydantic contracts, manual commits | done |
| 3 | Agent runtime: worker, retry, idempotency, DLQ, timeouts | next |
| 4 | LangGraph: state, dispatch node, results consumer, checkpointer | |
| 5 | Persistence: tables, repositories, outbox, approvals | |
| 6 | Integrations: GitHub and Jira clients, tools, webhooks, workspaces | |
| 7 | Automation workflows: `jira-to-pr`, `pr-review`, `pr-command` | |
| 8 | Parallelism: fan-out, reducers, fan-in | |
| 9 | Production: tracing, metrics, rate limits, deployment | |

## Requirements

Docker with Compose, Python 3.12 or newer, and `uv` (`python -m pip install uv`).

## Getting started

```bash
cp .env.example .env          # optional; the defaults already work locally
make install                  # sync the uv workspace
make up                       # redpanda, console, postgres, redis
make topics                   # confirm the six topics exist
make test                     # unit + integration
```

Redpanda Console is at <http://localhost:8080>. It is the fastest way to inspect
`agent.dlq` and `agent.events` while developing.

Host ports avoid the usual collisions, since 5432 and 6379 are often already taken:

| Service | Host port | Override |
|---------|-----------|----------|
| Redpanda (Kafka API) | 19092 | `REDPANDA_PORT` |
| Redpanda Console | 8080 | `CONSOLE_PORT` |
| PostgreSQL | 5452 | `POSTGRES_PORT` |
| Redis | 6381 | `REDIS_PORT` |

## Layout

```text
apps/          api, orchestrator, worker, outbox_worker
packages/      contracts, messaging, database, observability, shared, ...
config/        agents.yaml, trigger_rules.yaml
docker/        compose.yaml and the Redpanda topic bootstrap
```

`packages/contracts` and `packages/shared` depend on nothing internal, and no package
imports from `apps/`.

## Testing

Unit tests need no containers. Integration tests talk to the real broker and are marked
accordingly.

```bash
make test-unit     # no docker required
make test-int      # needs `make up`
make check         # lint, format check, unit tests
```

## Design notes worth knowing

**The broker is an implementation detail.** Everything goes through `MessageBus`. The
adapter speaks the Kafka protocol, so Apache Kafka or a managed service is a configuration
change. Nothing outside `packages/messaging` imports a broker client.

**Offsets are committed only after a handler returns.** A worker killed mid-task leaves its
offset untouched, so the task is redelivered and the idempotency check decides whether to
re-run it. A handler that raises causes a seek back to that offset, because the consumer's
in-memory position has already moved past it.

**One run maps to one partition.** The partition key is the workflow run ID, so the tasks of
a run stay ordered. Partition count therefore caps how many workers of one agent type can
run concurrently; `agent.tasks` has 12.

**A malformed payload is skipped, not retried.** It can never become valid, and blocking its
partition would stall every run that hashes to it. It is logged at error level.
