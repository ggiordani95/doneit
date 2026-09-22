# AI Agent Swarm — Technical Specification

**Status:** Draft / v1.0  
**Language:** Python 3.12+  
**Orchestration:** LangGraph  
**Messaging:** Redpanda (Kafka-protocol compatible)  
**Persistence:** PostgreSQL  
**Cache / coordination:** Redis  
**Integrations:** GitHub, Jira  

> **History.** v0.1 targeted TypeScript / Node.js with Apache Kafka. v0.2 ported the design to
> Python and added the GitHub and Jira integrations. This version, v1.0, reorganizes the whole
> document into the canonical 19-section outline and adopts **Redpanda** as the broker.
> Both previous revisions are kept under `archive/`.
>
> **Terminology.** "Kafka protocol" means topics, partitions, consumer groups and offsets.
> The broker we run is **Redpanda**; Apache Kafka and managed Kafka services are drop-in
> replacements at the configuration level. No application code depends on which one is running.

---

## Table of Contents

1. [Objetivo](#1-objetivo)
2. [Arquitetura](#2-arquitetura)
3. [Conceitos](#3-conceitos)
4. [LangGraph](#4-langgraph)
5. [Redpanda](#5-redpanda)
6. [Agent Protocol](#6-agent-protocol)
7. [Task Lifecycle](#7-task-lifecycle)
8. [State Management](#8-state-management)
9. [Agent Discovery / Routing](#9-agent-discovery--routing)
10. [Parallel Execution](#10-parallel-execution)
11. [Failure / Retry](#11-failure--retry)
12. [Idempotency](#12-idempotency)
13. [Observability](#13-observability)
14. [Persistence](#14-persistence)
15. [Security](#15-security)
16. [API](#16-api)
17. [Python Interfaces](#17-python-interfaces)
18. [Repository Structure](#18-repository-structure)
19. [Implementation Phases](#19-implementation-phases)

---

# 1. Objetivo

Build a distributed multi-agent AI execution platform where:

- **LangGraph** owns workflow orchestration and workflow state.
- **Redpanda** is the asynchronous communication backbone, over the Kafka protocol.
- **Agent workers** execute specialized AI tasks.
- **PostgreSQL** stores durable application and workflow data.
- **Redis** provides locks, short-lived state, rate limiting and cancellation signals.
- **GitHub and Jira** are first-class integrations: workflows can be *triggered by* them
  through webhooks and can *act on* them through tools.
- Agents are independently deployable and horizontally scalable.

## 1.1 Requirements

The platform must support:

| # | Capability |
|---|------------|
| 1 | Sequential agent execution |
| 2 | Parallel agent execution (fan-out / fan-in) |
| 3 | Dynamic routing between agents |
| 4 | Agent retries with backoff |
| 5 | Task idempotency |
| 6 | Failure recovery after process death |
| 7 | Dead-letter handling |
| 8 | Workflow persistence across restarts |
| 9 | Observability: tracing, metrics, structured logs |
| 10 | Human approval steps |
| 11 | New agent types without changing the orchestration core |
| 12 | Workflows triggered by Jira issue events and GitHub pull request events |
| 13 | Agents that read and create branches, commit, open and update PRs, transition issues |

## 1.2 Non-Goals

The platform is **not**:

- a generic chatbot framework;
- a replacement for Redpanda/Kafka or PostgreSQL;
- an LLM provider;
- a CI system — it reacts to check results, it does not run CI;
- an unrestricted autonomous agent network.

Its single responsibility:

> **Reliable distributed orchestration and execution of specialized AI agents, connected to
> the team's real delivery tools.**

## 1.3 Definition of Done (MVP)

The MVP is complete when a Jira issue transition produces, with no human input other than
the final merge, a **draft pull request on GitHub** that:

- was implemented by the coder agent on a dedicated branch;
- has a review posted by the reviewer agent;
- is linked from the Jira issue, which was transitioned to *In Review*;

and the run survives all three of:

- a worker killed mid-execution — the task is recovered with no duplicate branch or PR;
- a duplicate webhook delivery — no duplicate run;
- a GitHub HTTP 429 — retried with backoff.

---

# 2. Arquitetura

## 2.1 Separation of Responsibilities

| Layer | Owns | Must NOT contain |
|-------|------|------------------|
| **LangGraph** | Workflow state, transitions, routing, branching, loops, aggregation, human-in-the-loop | Broker or HTTP implementation details |
| **Redpanda** | Task delivery, event publication, decoupling, consumer groups, partitioning, replay | Business decisions |
| **Agent Workers** | Executing one task at a time, publishing results | The decision of what runs next |
| **Integrations** | Auth, API translation, webhook normalization, HTTP-level retry | Workflow decisions |
| **PostgreSQL** | Durable relational state | Transport concerns |

The governing rule:

> **LangGraph decides. Redpanda transports. Workers execute. Integrations act. PostgreSQL persists.**

## 2.2 System Diagram

```text
     ┌──────────────┐        ┌──────────────┐
     │    GitHub    │        │     Jira     │
     └──────┬───────┘        └──────┬───────┘
            │ webhooks              │ webhooks
            ▼                       ▼
         ┌──────────────────────────────┐
         │        API (FastAPI)         │
         │   /workflows   /webhooks/*   │
         └──────────────┬───────────────┘
                        ▼
         ┌──────────────────────────────┐
         │       Workflow Service       │
         └──────────────┬───────────────┘
                        ▼
         ┌──────────────────────────────┐
         │    LangGraph Orchestrator    │
         └──────────────┬───────────────┘
                        │ commands / events
                        ▼
         ┌──────────────────────────────┐
         │           Redpanda           │
         └──────────────┬───────────────┘
     ┌──────────────────┼──────────────────┐
     ▼                  ▼                  ▼
┌──────────┐      ┌──────────┐      ┌──────────┐
│ Research │      │  Coder   │      │ Reviewer │
│  Worker  │      │  Worker  │      │  Worker  │
└────┬─────┘      └────┬─────┘      └────┬─────┘
     │                 │ tools            │ tools
     │                 ▼                  ▼
     │          ┌──────────────┐   ┌──────────────┐
     │          │ GitHub / git │   │ GitHub / Jira│
     │          │   adapters   │   │   adapters   │
     │          └──────────────┘   └──────────────┘
     └──────────────────┼──────────────────┘
                        ▼
                     Redpanda
                        ▼
                Workflow Service
                        ▼
                    PostgreSQL
```

## 2.3 Technology Stack

| Concern | Choice | Notes |
|---------|--------|-------|
| Language | Python 3.12+ | `asyncio` throughout; no blocking I/O in workers |
| Packaging | `uv` workspace | One `pyproject.toml` per package, single lockfile |
| Orchestration | `langgraph`, `langgraph-checkpoint-postgres` | `AsyncPostgresSaver` for checkpoints |
| Broker | **Redpanda** | Single binary, no ZooKeeper/KRaft, built-in Console. See §5.1 |
| Broker client | `aiokafka` | Async-native, Kafka protocol, works unchanged against Redpanda |
| Contracts | `pydantic` v2 | Every broker payload, API body and tool I/O |
| API | `fastapi` + `uvicorn` | |
| Database | `sqlalchemy` 2.x async + `asyncpg` + `alembic` | |
| Cache / locks | `redis` (asyncio client) | Rate limits, branch locks, cancellation flags |
| LLM | Provider abstraction (§6.5) over `anthropic`, `openai` SDKs | |
| GitHub | `githubkit` | Async, typed from GitHub's OpenAPI |
| Jira | `httpx.AsyncClient` against Jira Cloud REST v3 | Thin adapter we own, including ADF conversion |
| Git | `git` CLI via `asyncio.create_subprocess_exec` | Full fidelity; `pygit2` optional later |
| Logging | `structlog` | JSON output |
| Tracing / metrics | `opentelemetry-sdk` + fastapi/sqlalchemy/httpx/aiokafka instrumentation | |
| Config | `pydantic-settings` | Env vars and `.env` |
| Tests | `pytest`, `pytest-asyncio`, `testcontainers`, `respx` | Redpanda and Postgres containers in CI |
| Quality | `ruff` (lint + format), `mypy --strict` | |

---

# 3. Conceitos

## 3.1 Agent

An agent is a specialized execution unit. Initial set:

```text
planner      researcher    coder       tester
reviewer     security-reviewer         documentation-writer
jira-triager
```

An agent is defined by its type, version, capabilities and permissions. The implementation
is a plain class satisfying the `Agent` protocol (§6.1). Agents never decide what runs next.

## 3.2 Task

A task is **one unit of agent work** — a *command* sent by the orchestrator.

```python
class AgentTask(BaseModel):
    id: str  # globally unique, uuid7 preferred
    workflow_id: str
    workflow_run_id: str
    agent_type: str
    agent_version: str | None
    input: dict[str, Any]
    metadata: TaskMetadata
    created_at: datetime
```

Wire format is `camelCase` so payloads stay compatible across revisions.

## 3.3 Workflow

A workflow is a versioned LangGraph graph, e.g. `jira-to-pr:v1`. A **workflow run** is one
execution of it.

```python
class WorkflowStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"  # human-in-the-loop or awaiting agent results
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
```

A running workflow keeps the version it started with unless explicitly migrated.

## 3.4 Event

An event states that **something happened**. It is immutable and past-tense.

Two families:

- **Internal:** `AgentTaskReceived`, `AgentTaskStarted`, `AgentTaskCompleted`,
  `AgentTaskFailed`, `AgentTaskRetried`, `AgentTaskDeadLettered`.
- **External (normalized):** `github.pull_request.opened`, `jira.issue.transitioned`, etc.

The command/event distinction is structural and must be preserved:

| | Command | Event |
|---|---------|-------|
| Means | "Do this" | "This happened" |
| Example | `AgentTaskRequested` | `AgentTaskCompleted` |
| Emitted by | Orchestrator | Workers and integrations |
| Topic | `agent.tasks` | `agent.results`, `agent.events`, `integration.*` |

## 3.5 Agent Run

An **agent run** is one attempt at executing one task by one worker. It is the unit that
carries idempotency and retry state, persisted as `task_executions`.

```text
task (logical unit of work)
 └── agent run  attempt 1  → failed (transient)
 └── agent run  attempt 2  → completed
```

A task may have several agent runs; only one may reach `completed`.

## 3.6 Shared State

Shared state is the `SwarmState` owned by LangGraph (§8). Rules:

- Workers receive an **explicitly selected slice** of it, never the full history.
- Workers never mutate it directly; they return results and the orchestrator merges them.
- Fields written by parallel branches declare a reducer so merges are deterministic.

---

# 4. LangGraph

## 4.1 Role

LangGraph answers exactly one question: **what happens next?** It owns transitions,
branching, loops, aggregation, workflow-level retries and human decisions.

## 4.2 Graph Definition

Each workflow is a Python module exporting a compiled `StateGraph` registered under a
versioned name.

```python
def build_graph(deps: Deps) -> CompiledGraph:
    g = StateGraph(SwarmState)

    g.add_node("fetch_issue", make_fetch_issue(deps.jira))
    g.add_node("planner", make_dispatch("planner", deps.bus))
    g.add_node("ensure_branch", make_ensure_branch(deps.github))
    g.add_node("dispatch", make_dispatch_dynamic(deps.bus))
    g.add_node("open_pr", make_open_pr(deps.github))
    g.add_node("jira_update", make_jira_update(deps.jira))
    g.add_node("await_approval", await_approval)

    g.set_entry_point("fetch_issue")
    g.add_edge("fetch_issue", "planner")
    g.add_edge("planner", "ensure_branch")
    g.add_conditional_edges("ensure_branch", fan_out, ["dispatch"])
    g.add_conditional_edges(
        "review", route_after_review, {"changes": "dispatch", "ok": "mark_ready"}
    )

    return g.compile(checkpointer=deps.checkpointer)
```

## 4.3 The Dispatch Pattern

LangGraph does not execute agents in-process. A `dispatch` node:

1. builds an `AgentTask`;
2. publishes it to `agent.tasks` through the `MessageBus`;
3. `interrupt()`s the graph, leaving the run in `WAITING`.

When the result arrives on `agent.results`, the results consumer resumes the run:

```python
await graph.ainvoke(
    Command(resume={"task_id": ev.task_id, "result": ev.result}),
    config={"configurable": {"thread_id": ev.workflow_run_id}},
)
```

This keeps the graph durable: the process can die between publish and resume without
losing the run, because the checkpoint holds the interrupt.

## 4.4 Checkpointing

`AsyncPostgresSaver` with `thread_id = workflow_run_id`. It creates and owns the
`checkpoints` and `checkpoint_writes` tables via `setup()`. Checkpoints are the source of
truth for *graph position*; PostgreSQL application tables are the source of truth for
*business records* (§14).

## 4.5 Human-in-the-Loop

`interrupt()` also implements approvals. The run enters `WAITING`, a `human_approvals` row
is written, and `POST /approvals/{id}/approve` resumes the graph with `Command(resume=...)`.
The run survives process restarts while waiting.

Mandatory approval gates (policy-configurable):

- `github.merge_pull_request` — always, in v1;
- pushing to a protected branch — never allowed for an agent at all;
- `jira.transition_issue` into a *Done*-category status.

---

# 5. Redpanda

## 5.1 Why Redpanda

Redpanda implements the Kafka wire protocol, so `aiokafka`, consumer groups, partitions,
offsets, headers and idempotent producers all work unchanged.

Advantages for this platform:

- a **single binary** — no ZooKeeper, no KRaft quorum to operate;
- a much **smaller footprint** on a dev machine that also runs PostgreSQL, Redis and
  LLM-calling workers;
- **Redpanda Console** — topic browser, consumer-group lag, message inspection. This is the
  primary tool for reading `agent.dlq` and `agent.events` during development;
- **`rpk`** for topic creation, ACLs and health checks.

Constraints we impose on ourselves:

- application code never imports a Redpanda-specific client and never uses Redpanda-only
  features (WASM transforms, Redpanda Connect) in the core path — everything goes through
  `MessageBus` (§17.6);
- swapping to Apache Kafka, MSK or Confluent Cloud must remain a config change
  (`KAFKA_BOOTSTRAP_SERVERS` plus SASL/TLS), exercised at least once in CI —
  `testcontainers` ships both `RedpandaContainer` and `KafkaContainer`.

Licensing note: Redpanda Community is source-available (BSL), not Apache 2.0. This does not
restrict running it as our own infrastructure; it matters only if the platform were resold
as a managed service.

## 5.2 Topics

| Topic | Kind | Contents |
|-------|------|----------|
| `agent.tasks` | command | Work for agent workers |
| `agent.results` | event | `AgentTaskCompleted` / `AgentTaskFailed` |
| `agent.events` | event | Lifecycle and observability |
| `agent.dlq` | event | Unprocessable tasks |
| `integration.github.events` | event | Normalized GitHub webhooks |
| `integration.jira.events` | event | Normalized Jira webhooks |

Later, if needed: `workflow.events`, `human.approvals`, `agent.tasks.retry`.

Topics, partition counts and retention are declared in `docker/redpanda/topics.yaml` and
applied by `docker/redpanda/bootstrap.sh` using `rpk topic create`. The same file drives
`kafka-topics.sh` when running against Apache Kafka.

## 5.3 Partitions

- **Key:** `workflow_run_id`. Tasks of the same run are ordered and land on the same
  partition.
- **Count:** a consumer group cannot have more active workers than partitions, so partitions
  bound the parallelism per agent type. Default **12** for `agent.tasks`; 6 for the rest.
- Raising partition count later is safe for throughput but changes key→partition mapping, so
  it must be done during a quiet window.

## 5.4 Consumer Groups

One group per agent type, plus the orchestrator's groups:

```text
agent-planner        agent-researcher      agent-coder
agent-reviewer       agent-tester

orchestrator-results
orchestrator-github-events
orchestrator-jira-events
```

Instances of the same agent share a group; partitions spread across them.

### Long-running tasks vs. rebalance

Agent tasks run for minutes, far longer than a normal consumer loop expects. This is the one
place the Kafka model works against this workload, so the worker must:

- **decouple execution from the poll loop** — insert the `task_executions` row, hand the task
  to a bounded `asyncio.TaskGroup`, return to polling immediately;
- **apply backpressure** — when the concurrency semaphore is exhausted, `consumer.pause()`
  the assigned partitions and `resume()` when a slot frees, which keeps heartbeats alive;
- **minimize partition movement** — `StickyPartitionAssignor`, so a partition stays with the
  consumer that already had it. Note that `aiokafka` 0.14 implements only the *eager*
  rebalance protocol, so a rebalance still revokes everything briefly; decoupled execution
  is what keeps that from interrupting work, not the assignor;
- on `on_partitions_revoked`, commit offsets of completed tasks and let in-flight tasks
  **finish** rather than cancelling them; the duplicate delivery to the new owner is absorbed
  by the idempotency check (§12).

```python
AIOKafkaConsumer(
    "agent.tasks",
    group_id=f"agent-{agent_type}",
    enable_auto_commit=False,
    partition_assignment_strategy=(StickyPartitionAssignor,),
    max_poll_records=concurrency,
    max_poll_interval_ms=int(task_timeout_s * 1000) + 60_000,
    session_timeout_ms=45_000,
    heartbeat_interval_ms=3_000,
)
```

## 5.5 Retry and DLQ

Delivery is **at-least-once**. Producers set `enable_idempotence=True`; consumers commit
offsets manually, only after the result has been published.

```text
broker message → validate → idempotency check → execute
   → persist result → publish result event → commit offset
```

Delayed retries re-publish to `agent.tasks` with `attempt + 1` and a `not_before` header;
the worker waits until `not_before` before executing. A dedicated `agent.tasks.retry` topic
with a scheduler consumer is an acceptable v2 refinement.

After `max_attempts`, or immediately for a permanent error, the task goes to `agent.dlq`
carrying the original task, the error, the attempt count, timestamps, agent type, workflow
ID and correlation ID. **No task disappears silently.**

## 5.6 Local Development

```yaml
# docker/compose.yaml (excerpt)
services:
  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda start
      - --smp 1
      - --memory 1G
      - --overprovisioned
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
    ports: ["19092:19092", "9644:9644"]
  redpanda-console:
    image: redpandadata/console:latest
    environment:
      KAFKA_BROKERS: redpanda:9092
    ports: ["8080:8080"]
```

---

# 6. Agent Protocol

## 6.1 The Contract

```python
class Agent(Protocol):
    type: str
    version: str

    async def execute(self, task: AgentTask, ctx: AgentContext) -> AgentResult: ...
```

Every agent is:

- **stateless** between tasks — any git working copy is ephemeral (§6.6);
- **idempotent** — the same `task_id` twice must not double any side effect;
- **cancellable** — it polls `ctx.cancelled()` during long work;
- **bounded** — it runs inside `asyncio.timeout(task_timeout_s)`.

## 6.2 Agent Context

```python
class AgentContext(BaseModel):
    task_id: str
    workflow_id: str
    workflow_run_id: str
    state: dict[str, Any]  # explicitly selected slice, never the full history
    tools: list[ToolSpec]  # already filtered by permissions
    workspace: Workspace | None
```

## 6.3 Tools

```python
class Tool(Protocol[I, O]):
    name: str
    description: str
    input_model: type[I]
    output_model: type[O]
    permission: Permission

    async def execute(self, input: I, ctx: ToolContext) -> O: ...
```

Tool I/O is Pydantic, so the JSON schema handed to the LLM comes from
`input_model.model_json_schema()`.

Namespaces:

```text
filesystem.read / write / list
shell.execute
git.clone / checkout / diff / commit / push / log
github.*        (§6.4)
jira.*          (§6.4)
web.search
database.query
```

## 6.4 Integration Tools

### GitHub

Authentication is a **GitHub App** with installation tokens, cached in Redis and refreshed
before expiry. Minimum permissions: `contents: write`, `pull_requests: write`,
`metadata: read`, `checks: read`, `issues: write`. Git pushes use the same token over HTTPS
via `GIT_ASKPASS`, never written into `.git/config`. A fine-grained PAT is acceptable for
local development only.

| Tool | Permission | Notes |
|------|-----------|-------|
| `github.list_branches` | `github:read` | |
| `github.get_branch` | `github:read` | |
| `github.read_file` | `github:read` | Read without cloning |
| `github.get_tree` | `github:read` | |
| `github.compare` | `github:read` | |
| `github.get_pull_request` | `github:read` | |
| `github.get_pr_diff` | `github:read` | Reviewer's main input |
| `github.list_check_runs` | `github:read` | |
| `github.ensure_branch` | `github:write` | Idempotent create |
| `github.open_pull_request` | `github:write` | Find-or-create by head; opens as **draft** |
| `github.update_pull_request` | `github:write` | Title, body, ready-for-review |
| `github.comment` | `github:write` | |
| `github.create_review` | `github:write` | `COMMENT` / `REQUEST_CHANGES`; `APPROVE` is policy-gated |
| `github.add_labels` | `github:write` | |
| `github.request_reviewers` | `github:write` | |
| `github.merge_pull_request` | `github:merge` | Human approval required (§4.5) |

### Jira

Authentication: API token (bot account, Basic auth) for Jira Cloud in v1; OAuth 2.0 3LO
later for multi-tenant. Jira Data Center uses a PAT. The adapter hides the difference and
converts Markdown to Atlassian Document Format.

| Tool | Permission | Notes |
|------|-----------|-------|
| `jira.get_issue` | `jira:read` | Summary, description (ADF→Markdown), acceptance criteria, links |
| `jira.search` | `jira:read` | JQL, result size capped |
| `jira.get_transitions` | `jira:read` | |
| `jira.add_comment` | `jira:write` | |
| `jira.add_remote_link` | `jira:write` | Links the PR to the issue |
| `jira.add_labels` | `jira:write` | |
| `jira.assign` | `jira:write` | |
| `jira.transition_issue` | `jira:write` | *Done*-category transitions need approval |
| `jira.create_issue` | `jira:write` | Reviewer files follow-ups |
| `jira.link_issues` | `jira:write` | |

## 6.5 Model Provider Abstraction

```python
class LlmProvider(Protocol):
    async def generate(self, request: LlmRequest) -> LlmResponse: ...
    async def generate_with_tools(
        self, request: LlmRequest, tools: list[ToolSpec]
    ) -> LlmResponse: ...
```

Agents are never coupled to one vendor. Implementations wrap `anthropic`, `openai`, Google
and local models; agent configuration selects provider and model. The abstraction may be
backed by LangChain's `BaseChatModel` so LangGraph prebuilt nodes remain usable.

## 6.6 Workspaces

Agents that modify code need a working copy.

```python
class WorkspaceManager(Protocol):
    async def acquire(self, repo: str, branch: str, base: str) -> Workspace: ...
    async def release(self, ws: Workspace) -> None: ...
```

- One workspace per task under `$SWARM_WORKSPACES/{task_id}`, deleted on release whether the
  task succeeded or failed. Never reused across tasks in v1.
- Shallow clone (`--depth 50 --single-branch`) against a per-repo bare mirror cache
  (`--reference-if-able`).
- `shell.execute` runs with `cwd=ws.path`, a timeout and a resource-limited subprocess
  (container or cgroup in production).
- Commits are authored by the bot, with the human requester as `Co-authored-by` when known.
- Workspace paths are never logged, because they may carry credentials.

---

# 7. Task Lifecycle

## 7.1 States

```text
PENDING
   │  orchestrator builds the task
   ▼
PUBLISHED
   │  written to agent.tasks
   ▼
RECEIVED
   │  worker consumed it, idempotency row inserted
   ▼
RUNNING
   │  agent executing, inside timeout
   ├──────────────┬──────────────┐
   ▼              ▼              ▼
COMPLETED      FAILED       CANCELLED
                  │
           retryable and attempts left?
             ┌────┴────┐
            yes        no
             │          │
             ▼          ▼
           RETRY       DLQ
             │
             └──► PUBLISHED (attempt + 1)
```

## 7.2 Worker Sequence

```text
startup
  → connect to Redpanda
  → register agent in the registry
  → consume
      → validate (Pydantic)
      → idempotency check
      → acquire workspace (if needed)
      → execute agent under timeout
      → persist result
      → publish result event
      → release workspace
      → commit offset
  → consume next
```

The offset is **never** committed before the result is durably published.

## 7.3 Timeouts

| Agent | Timeout |
|-------|---------|
| researcher | 5 min |
| reviewer | 10 min |
| tester | 20 min |
| coder | 30 min |

A timeout produces a controlled transient failure, not a crash.

## 7.4 Graceful Shutdown

`SIGTERM` / `SIGINT` via `loop.add_signal_handler`: stop consuming, drain in-flight tasks up
to `SHUTDOWN_GRACE_S`, commit offsets, release workspaces, close producers, exit.

---

# 8. State Management

## 8.1 SwarmState

```python
class SwarmState(TypedDict, total=False):
    task: str

    # external context
    jira_issue_key: str | None
    repo: str | None  # "owner/name"
    base_branch: str | None
    work_branch: str | None
    pull_request_number: int | None

    plan: list[PlanStep]
    research: Annotated[list[ResearchResult], operator.add]
    code_changes: Annotated[list[CodeChange], operator.add]
    test_results: Annotated[list[TestResult], operator.add]
    review: ReviewResult | None

    review_iterations: int
    current_agent: str | None
    errors: Annotated[list[AgentError], operator.add]
```

## 8.2 Rules

- **Reducers are mandatory** for any field written by more than one parallel branch. Without
  one, concurrent writes raise `InvalidUpdateError`.
- **Checkpoint is graph position**, PostgreSQL is business record. Never infer business
  state by reading checkpoints.
- **Context is selected, not dumped.** A dispatch node copies only the slice the agent needs
  into `task.input`.
- **State is bounded.** Large artifacts (diffs, logs, research dumps) are stored externally
  and referenced by ID or URL, never inlined into state.

## 8.3 Three Levels of State

| Level | Store | Lifetime | Example |
|-------|-------|----------|---------|
| Graph position | LangGraph checkpoints (Postgres) | Until run completes | Which node is next, pending interrupts |
| Business record | PostgreSQL tables | Permanent | Runs, tasks, approvals, external references |
| Coordination | Redis | Seconds to minutes | Branch locks, rate-limit buckets, cancel flags |

---

# 9. Agent Discovery / Routing

## 9.1 Agent Registry

```python
class AgentRegistration(BaseModel):
    id: str
    type: str
    version: str
    capabilities: list[str]
    permissions: list[Permission]
    status: Literal["active", "disabled"] = "active"
```

```yaml
# config/agents.yaml
- id: coder-01
  type: coder
  version: "1.2.0"
  capabilities: [python, fastapi, sqlalchemy]
  permissions: [filesystem, shell, git, "github:read", "github:write", "jira:read"]
  status: active
```

Static configuration in v1. Dynamic discovery — workers announcing themselves on
`agent.events` with a heartbeat TTL — is a later addition and must not change the routing
interface.

## 9.2 Capability Routing

```python
agent_type = registry.select(capability="code-review", language="python")
# -> "reviewer"
```

Selection is deterministic: filter by capability and status, then take the highest active
version. No LLM decides which agent runs; the graph does.

## 9.3 External Triggers

Normalized external events select a workflow through declarative rules — configuration, not
code.

```yaml
# config/trigger_rules.yaml
- name: jira-ready-for-dev
  when:
    event_type: jira.issue.transitioned
    to_status: "Ready for Dev"
    labels_any: ["ai-agent"]
  start:
    workflow: jira-to-pr:v1
    input_from_event: true

- name: pr-opened-review
  when:
    event_type: github.pull_request.opened
    repos: ["acme/*"]
  start:
    workflow: pr-review:v1

- name: pr-slash-command
  when:
    event_type: github.issue_comment.created
    body_prefix: "/swarm"
  start:
    workflow: pr-command:v1
```

## 9.4 Built-in Workflows

### `jira-to-pr:v1`

```text
Jira issue → "Ready for Dev"
      ▼
  fetch_issue        jira.get_issue → state.task, state.jira_issue_key
      ▼
  planner            plan, target repo, branch "feature/PROJ-42-<slug>"
      ▼
  ensure_branch      github.ensure_branch from base_branch
      ├──────────────┐
      ▼              ▼
  researcher       coder      clone workspace, implement, commit, push
      └──────┬───────┘
             ▼
          tester     run suite in a fresh workspace
             ▼
      open_draft_pr  "[PROJ-42] ..." draft PR, body carries plan + test summary
             ▼
       jira_update   remote link + comment + transition to "In Review"
             ▼
        reviewer     github.get_pr_diff → github.create_review
             │
      ┌──────┴──────┐
   changes         ok
  requested         ▼
      │         mark_ready   draft = false
      └─► coder      ▼
   (max 2 loops)  COMPLETED
```

### `pr-review:v1`

PR opened or synchronized → reviewer and security-reviewer in parallel → aggregate →
`github.create_review` → completed.

### `pr-command:v1`

`/swarm fix-tests`, `/swarm address-review`, `/swarm rebase` in a PR comment. Resolves the
PR's branch and Jira key from the branch name or the `[PROJ-42]` title prefix, runs the
matching sub-graph on that branch and pushes.

---

# 10. Parallel Execution

## 10.1 Fan-Out

```python
def fan_out(state: SwarmState) -> list[Send]:
    return [
        Send("dispatch", {"agent_type": "researcher", **slice_for("researcher", state)}),
        Send("dispatch", {"agent_type": "coder", **slice_for("coder", state)}),
    ]
```

Each branch publishes an independent task to `agent.tasks`. Because the partition key is
`workflow_run_id`, the tasks of one run stay ordered relative to each other.

## 10.2 Fan-In

The graph resumes once per arriving result and proceeds past the join only when every
expected `task_id` has reported. The set of expected IDs lives in the checkpoint, so a
crash mid-fan-in is recoverable.

Results merge through the `operator.add` reducers of §8.1 — never by overwriting.

## 10.3 Bounded Loops

The review loop is capped by `state["review_iterations"]`, default max 2. Exceeding it
routes to a human approval node rather than looping forever.

## 10.4 Concurrency Limits

- Per worker: `asyncio.Semaphore(concurrency)`, e.g. coder 4.
- Per provider/model/tenant: Redis token buckets (§15.4).
- Parallelism per agent type is ultimately bounded by partition count (§5.3).

---

# 11. Failure / Retry

## 11.1 Classification

| Transient (retry) | Permanent (DLQ or fail the run) |
|-------------------|--------------------------------|
| Provider timeout | Invalid task payload |
| Temporary network failure | Unsupported agent type |
| HTTP 429 / secondary rate limit | Malformed input |
| Broker unavailable | Invalid workflow state |
| GitHub or Jira 5xx | GitHub or Jira 401 / 403 / 404 |
| Non-fast-forward push | Missing required permission |

## 11.2 Policy

```python
class RetryPolicy(BaseModel):
    max_attempts: int = 4
    initial_delay_s: float = 1.0
    max_delay_s: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
```

Attempts: 1s → 2s → 4s → 8s, with jitter. A rate-limit response overrides the computed
delay with the provider's `Retry-After` or `X-RateLimit-Reset`.

## 11.3 Failure Isolation

An agent failure must never crash the swarm. The broker retains the task, another worker
picks it up, and the deployment platform restarts dead processes.

```text
coder worker dies mid-task
        ▼
offset was never committed
        ▼
another worker consumes the task
        ▼
idempotency check finds the incomplete run → executes
        ▼
ensure_branch / find-or-create PR absorb the partial side effects
```

## 11.4 Cancellation

`POST /workflows/{id}/cancel` → the graph cancels pending nodes and sets `cancel:{run_id}`
in Redis. Workers poll it during long work, kill `git` and `shell` subprocesses, release
workspaces and report `CANCELLED`. A draft PR already opened gets a "cancelled" comment and
label.

---

# 12. Idempotency

## 12.1 Internal

Every task has a globally unique `task_id`. On receipt the worker performs
`INSERT ... ON CONFLICT (task_id) DO NOTHING` into `task_executions`.

```text
task_id already completed
        ▼
do not execute again
        ▼
re-publish the stored result → commit offset
```

```python
class TaskExecution(BaseModel):
    task_id: str
    status: Literal["running", "completed", "failed"]
    attempt: int
    started_at: datetime
    completed_at: datetime | None
    result: dict[str, Any] | None
```

## 12.2 External Side Effects

Idempotency must extend past our own database, because a retry re-runs real API calls.

| Operation | Made idempotent by |
|-----------|--------------------|
| Create branch | `get_branch` first — "ensure branch exists" |
| Open PR | `find_pull_request(head, base)` first — find-or-create |
| Push | Rebase on remote first; non-fast-forward is a transient failure |
| Jira transition | Read current status; no-op if already there |
| Jira remote link | `globalId = "swarm:pr:{repo}#{number}"` — Jira upserts on it |
| Jira comment | Hidden marker `<!-- swarm:{task_id} -->`; skip if present |
| PR review | Search existing reviews for the marker before posting |

## 12.3 Webhook Deduplication

Providers redeliver. Each delivery is recorded in `webhook_deliveries` keyed by
`(provider, delivery_id)` with `ON CONFLICT DO NOTHING`; a duplicate is acknowledged with
`202` and dropped before it can start a second run.

- GitHub: `X-GitHub-Delivery`.
- Jira: `(webhookEvent, issue.id, timestamp)`, since Jira Cloud sends no delivery ID.

## 12.4 Branch Concurrency

A Redis lock `lock:github:{repo}:{branch}` serializes writers on the same branch so two
agents cannot push interleaved commits.

---

# 13. Observability

## 13.1 Trace Fields

Every log line and every span carries:

```text
workflow_id      workflow_run_id    task_id       agent_type
attempt          correlation_id     causation_id  tenant_id
```

Plus, when present: `jira_issue_key`, `repo`, `pull_request_number`.

## 13.2 Correlation and Causation

```text
WorkflowStarted            correlation_id = W1
      ▼
AgentTaskRequested         correlation_id = W1, causation_id = WorkflowStarted
      ▼
AgentTaskCompleted         correlation_id = W1, causation_id = AgentTaskRequested
```

`correlation_id` identifies the whole run; `causation_id` identifies the event that caused
this one. For Jira-triggered runs the correlation ID embeds the issue key
(`PROJ-42:run-123`) so traces are searchable by ticket.

## 13.3 Metrics

```text
workflow.duration / workflow.success / workflow.failure
agent.execution.duration / success / failure
broker.task.lag / broker.task.retry / broker.task.dlq
llm.request.duration / llm.request.tokens / llm.request.cost
integration.request.duration{provider,operation} / errors / rate_limited
webhook.received{provider,event_type} / webhook.rejected
workspace.acquire.duration / workspace.active
```

Exported through OpenTelemetry to Prometheus. Consumer-group lag is also visible in
Redpanda Console.

## 13.4 Logging

Structured JSON via `structlog`:

```json
{
  "level": "info",
  "event": "github.pull_request.opened",
  "workflow_run_id": "run-123",
  "task_id": "task-123",
  "repo": "acme/backend",
  "pull_request_number": 17,
  "jira_issue_key": "PROJ-42",
  "duration_ms": 842
}
```

**Never logged:** API keys, access tokens, passwords, webhook secrets, workspace URLs
containing credentials, or prompt content unless explicitly allowed by policy.

## 13.5 Tracing

One trace per workflow run. Spans: API request → graph node → task publish → worker consume
→ agent execute → LLM call → tool call → result publish. Context propagates through broker
message headers (`traceparent`).

---

# 14. Persistence

## 14.1 Tables

```text
workflows                definitions and versions
workflow_runs            one row per execution
tasks                    logical units of work
task_executions          one row per attempt; idempotency key
agents                   registry snapshot
human_approvals          approval requests and decisions
external_references      run ↔ jira issue / PR / branch
webhook_deliveries       (provider, delivery_id) dedup
integrations             tenant, provider, installation/site, credentials reference
outbox                   events pending publication
```

Plus LangGraph's `checkpoints` and `checkpoint_writes`, created by `AsyncPostgresSaver.setup()`.

```text
workflows
   └── workflow_runs
           ├── tasks ── task_executions
           ├── external_references
           └── human_approvals
```

## 14.2 Division of Responsibility

Redpanda is **transport**; PostgreSQL is **durable state**. Never rely on broker retention
for application-critical facts. Anything a human or an audit may ask about later lives in
PostgreSQL.

## 14.3 Transactional Publication (Outbox)

When a database transaction produces an event that must not be lost:

```text
PostgreSQL transaction
   ├── update workflow_run
   └── insert outbox row
             ▼
   outbox publisher polls → publishes to Redpanda → marks sent
```

This prevents the classic inconsistency where the DB commit succeeds and the publish fails.
The publisher is its own process, `apps/outbox_worker`.

## 14.4 External References

```python
class ExternalReference(BaseModel):
    run_id: str
    provider: Literal["github", "jira"]
    kind: Literal["jira_issue", "github_pr", "github_branch"]
    external_id: str  # "PROJ-42" or "acme/backend#17"
    url: str
```

This is what makes `GET /runs/by-external/jira/PROJ-42` possible and what lets an incoming
PR event find the run that created the PR.

---

# 15. Security

## 15.1 Permission Model

```python
class Permission(StrEnum):
    FILESYSTEM = "filesystem"
    SHELL = "shell"
    NETWORK = "network"
    GIT = "git"
    DATABASE = "database"
    GITHUB_READ = "github:read"
    GITHUB_WRITE = "github:write"
    GITHUB_MERGE = "github:merge"
    JIRA_READ = "jira:read"
    JIRA_WRITE = "jira:write"
```

The tool registry filters tools by the agent's permissions **before** exposing them to the
LLM, so an unpermitted tool is not merely refused, it is invisible.

| Agent | Typical permissions |
|-------|--------------------|
| researcher | `network`, `github:read`, `jira:read` |
| coder | `filesystem`, `shell`, `git`, `github:read`, `github:write`, `jira:read` |
| reviewer | `github:read`, `github:write`, `jira:read` — no `shell`, no `git` |
| tester | `filesystem`, `shell`, `git`, `github:read` |

`github:merge` is granted to no agent in v1. Pushing to a protected branch is never allowed.

## 15.2 Credentials

- Secrets live in the environment or a secret store (Vault). The `integrations` table stores
  a **reference**, never a secret.
- GitHub App installation tokens are short-lived, cached in Redis, refreshed before expiry.
- Git credentials are injected via `GIT_ASKPASS` and never written to disk in `.git/config`.

## 15.3 Webhook Security

- **GitHub:** verify `X-Hub-Signature-256` (HMAC-SHA256) against the app webhook secret;
  reject otherwise. Compare with `hmac.compare_digest`.
- **Jira:** Jira Cloud does not sign bodies, so a secret in the URL query string is
  mandatory, optionally combined with an Atlassian source-IP allowlist.
- Both endpoints are rate-limited and return `202` without doing workflow work inline.

## 15.4 Rate Limiting

Redis token buckets at several levels: global, per provider, per model, per agent, per
tenant. The same mechanism covers LLM providers, GitHub (`rl:github:{installation_id}`,
honoring `X-RateLimit-Remaining` / `Reset`) and Jira.

## 15.5 Prompt Injection

Issue descriptions, PR bodies and comments are **untrusted input written by third parties**.

- External text is wrapped as data in prompts, never concatenated as instructions.
- Side-effecting tools (`github:write`, `jira:write`) are callable from graph nodes designed
  for them, not from a free-form agent tool loop, unless the agent's policy explicitly allows
  it.
- The coder agent never executes commands found in issue or PR text.

## 15.6 Multi-Tenancy

`tenant_id` propagates API → LangGraph → broker headers → worker → database. Integration
credentials are resolved per tenant, and database isolation policies are applied at the
persistence layer.

---

# 16. API

## 16.1 Endpoints

```text
POST /workflows                          start a run
GET  /workflows/{id}
GET  /workflows/{id}/runs/{run_id}
POST /workflows/{id}/cancel
POST /tasks/{id}/retry
POST /approvals/{id}/approve
POST /approvals/{id}/reject

POST /webhooks/github                    §15.3
POST /webhooks/jira                      §15.3

GET  /integrations                       configured integrations, no secrets
POST /integrations/github/installations  register a GitHub App installation
POST /integrations/jira/sites            register a Jira site and credentials reference
GET  /runs/by-external/{provider}/{id}   e.g. /runs/by-external/jira/PROJ-42

GET  /health                             liveness
GET  /ready                              broker + database reachable
```

## 16.2 Starting a Run

```http
POST /workflows
```

```json
{
  "workflow": "jira-to-pr:v1",
  "input": {
    "jiraIssueKey": "PROJ-42",
    "repo": "acme/backend",
    "baseBranch": "main"
  }
}
```

```json
{ "workflowRunId": "run-123", "status": "pending" }
```

## 16.3 Webhook Handling

Both webhook endpoints follow the same four steps and nothing more:

1. **Verify** signature or shared secret.
2. **Deduplicate** on the delivery ID (§12.3).
3. **Normalize** into an `IntegrationEvent` and publish to `integration.{provider}.events`.
4. **Return `202`.**

No workflow logic runs inline. Matching events to workflows is the orchestrator's job (§9.3).

GitHub events consumed initially:

```text
pull_request          opened, synchronize, reopened, closed, ready_for_review
pull_request_review   submitted
issue_comment         created          → "/swarm ..." commands
check_suite           completed
push
```

Jira events consumed initially:

```text
jira:issue_created
jira:issue_updated    status changes, label added, assignee = bot
comment_created       → "@swarm ..." commands
```

## 16.4 Errors

RFC 7807 problem documents:

```json
{
  "type": "https://swarm.internal/errors/workflow-not-found",
  "title": "Workflow not found",
  "status": 404,
  "detail": "No workflow registered under 'jira-to-pr:v9'"
}
```

---

# 17. Python Interfaces

This section consolidates the normative type definitions. All models are Pydantic v2 and
serialize to `camelCase` on the wire:

```python
model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
```

## 17.1 Identity and Metadata

```python
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict, Field


class EventMetadata(BaseModel):
    correlation_id: str
    causation_id: str | None = None
    tenant_id: str | None = None


class TaskMetadata(EventMetadata):
    attempt: int = 1


class ErrorInfo(BaseModel):
    code: str
    message: str
    retryable: bool = False
```

## 17.2 Agents and Tasks

```python
class AgentDefinition(BaseModel):
    id: str
    version: str
    capabilities: list[str]


class AgentTask(BaseModel):
    id: str
    workflow_id: str
    workflow_run_id: str
    agent_type: str
    agent_version: str | None = None
    input: dict[str, Any]
    metadata: TaskMetadata
    created_at: datetime = Field(default_factory=datetime.utcnow)


class AgentResult(BaseModel):
    task_id: str
    output: dict[str, Any]
    artifacts: list[str] = []  # references, never inlined blobs
    usage: dict[str, Any] | None = None


class AgentContext(BaseModel):
    task_id: str
    workflow_id: str
    workflow_run_id: str
    state: dict[str, Any]
    tools: list["ToolSpec"]
    workspace: "Workspace | None" = None

    def cancelled(self) -> bool: ...


class Agent(Protocol):
    type: str
    version: str

    async def execute(self, task: AgentTask, ctx: AgentContext) -> AgentResult: ...
```

## 17.3 Events

```python
class AgentTaskRequested(BaseModel):
    event_type: Literal["AgentTaskRequested"] = "AgentTaskRequested"
    event_version: int = 1
    task_id: str
    workflow_id: str
    workflow_run_id: str
    agent_type: str
    input: dict[str, Any]
    metadata: TaskMetadata
    created_at: datetime


class AgentTaskCompleted(BaseModel):
    event_type: Literal["AgentTaskCompleted"] = "AgentTaskCompleted"
    event_version: int = 1
    task_id: str
    workflow_id: str
    workflow_run_id: str
    agent_type: str
    result: dict[str, Any]
    metadata: TaskMetadata
    completed_at: datetime


class AgentTaskFailed(BaseModel):
    event_type: Literal["AgentTaskFailed"] = "AgentTaskFailed"
    event_version: int = 1
    task_id: str
    workflow_id: str
    workflow_run_id: str
    error: ErrorInfo
    metadata: TaskMetadata


AgentResultEvent = Annotated[
    AgentTaskCompleted | AgentTaskFailed,
    Field(discriminator="event_type"),
]


class IntegrationEvent(BaseModel):
    event_type: str  # "github.pull_request.opened"
    event_version: int = 1
    source: Literal["github", "jira"]
    delivery_id: str
    occurred_at: datetime
    tenant_id: str | None = None
    subject: dict[str, Any]  # repo/PR or issue identifiers
    payload: dict[str, Any]  # trimmed provider payload
    metadata: EventMetadata


class WorkflowTrigger(BaseModel):
    source: Literal["api", "github", "jira", "schedule"]
    external_id: str | None = None
    delivery_id: str | None = None
```

## 17.4 Workflow

```python
class WorkflowStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkflowRun(BaseModel):
    id: str
    workflow_id: str
    status: WorkflowStatus
    state: dict[str, Any]
    trigger: WorkflowTrigger | None = None
    created_at: datetime
    updated_at: datetime
```

## 17.5 Tools and Permissions

```python
I = TypeVar("I", bound=BaseModel)
O = TypeVar("O", bound=BaseModel)


class Permission(StrEnum):
    FILESYSTEM = "filesystem"
    SHELL = "shell"
    NETWORK = "network"
    GIT = "git"
    DATABASE = "database"
    GITHUB_READ = "github:read"
    GITHUB_WRITE = "github:write"
    GITHUB_MERGE = "github:merge"
    JIRA_READ = "jira:read"
    JIRA_WRITE = "jira:write"


class ToolSpec(BaseModel):
    name: str
    description: str
    input_schema: dict[str, Any]
    permission: Permission


class Tool(Protocol[I, O]):
    name: str
    description: str
    input_model: type[I]
    output_model: type[O]
    permission: Permission

    async def execute(self, input: I, ctx: "ToolContext") -> O: ...
```

## 17.6 Messaging

```python
from collections.abc import Awaitable, Callable

T = TypeVar("T", bound=BaseModel)
MessageHandler = Callable[[T, "MessageMeta"], Awaitable[None]]


class MessageBus(Protocol):
    async def publish(
        self,
        topic: str,
        message: BaseModel,
        *,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None: ...

    async def subscribe(
        self,
        topic: str,
        group_id: str,
        model: type[T],
        handler: MessageHandler[T],
    ) -> None: ...

    async def start(self) -> None: ...
    async def stop(self) -> None: ...


class RedpandaMessageBus:
    """aiokafka implementation. Idempotent producer, manual offset commit,
    cooperative sticky assignment. Speaks the Kafka protocol, so it also
    runs unchanged against Apache Kafka."""
```

## 17.7 Workers

```python
class AgentWorker(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...  # graceful: drain, commit, release


class AgentExecutor(Protocol):
    async def execute(self, task: AgentTask) -> AgentResult: ...


class RetryPolicy(BaseModel):
    max_attempts: int = 4
    initial_delay_s: float = 1.0
    max_delay_s: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True


class WorkerConfig(BaseModel):
    agent_type: str
    concurrency: int = 4
    task_timeout_s: float = 600
    shutdown_grace_s: float = 60
    retry: RetryPolicy = RetryPolicy()
```

## 17.8 Integrations

```python
class GitHubClient(Protocol):
    async def get_repo(self, repo: str) -> Repo: ...
    async def list_branches(self, repo: str) -> list[Branch]: ...
    async def get_branch(self, repo: str, name: str) -> Branch | None: ...
    async def create_branch(self, repo: str, name: str, from_sha: str) -> Branch: ...
    async def compare(self, repo: str, base: str, head: str) -> Comparison: ...
    async def get_file(self, repo: str, path: str, ref: str) -> FileContent | None: ...
    async def get_tree(self, repo: str, ref: str, recursive: bool = False) -> list[TreeEntry]: ...
    async def find_pull_request(self, repo: str, head: str, base: str) -> PullRequest | None: ...
    async def create_pull_request(self, repo: str, spec: PullRequestSpec) -> PullRequest: ...
    async def update_pull_request(
        self, repo: str, number: int, patch: PullRequestPatch
    ) -> PullRequest: ...
    async def get_pull_request_diff(self, repo: str, number: int) -> str: ...
    async def create_review(self, repo: str, number: int, review: ReviewSpec) -> Review: ...
    async def comment(self, repo: str, number: int, body: str) -> Comment: ...
    async def add_labels(self, repo: str, number: int, labels: list[str]) -> None: ...
    async def request_reviewers(
        self, repo: str, number: int, users: list[str], teams: list[str]
    ) -> None: ...
    async def list_check_runs(self, repo: str, ref: str) -> list[CheckRun]: ...
    async def merge_pull_request(
        self, repo: str, number: int, method: MergeMethod
    ) -> MergeResult: ...


class JiraClient(Protocol):
    async def get_issue(self, key: str, fields: list[str] | None = None) -> Issue: ...
    async def search(self, jql: str, max_results: int = 50) -> list[Issue]: ...
    async def create_issue(self, spec: IssueSpec) -> Issue: ...
    async def update_issue(self, key: str, fields: dict[str, Any]) -> None: ...
    async def add_comment(self, key: str, body: str) -> Comment: ...
    async def get_transitions(self, key: str) -> list[Transition]: ...
    async def transition_issue(
        self, key: str, transition: str | int, fields: dict[str, Any] | None = None
    ) -> None: ...
    async def assign(self, key: str, account_id: str | None) -> None: ...
    async def add_remote_link(self, key: str, url: str, title: str) -> None: ...
    async def link_issues(self, inward: str, outward: str, link_type: str) -> None: ...
    async def add_labels(self, key: str, labels: list[str]) -> None: ...


class WorkspaceManager(Protocol):
    async def acquire(self, repo: str, branch: str, base: str) -> "Workspace": ...
    async def release(self, ws: "Workspace") -> None: ...


class LlmProvider(Protocol):
    async def generate(self, request: LlmRequest) -> LlmResponse: ...
    async def generate_with_tools(
        self, request: LlmRequest, tools: list[ToolSpec]
    ) -> LlmResponse: ...
```

---

# 18. Repository Structure

`uv` workspace monorepo. Every package is importable as `swarm_<name>` and owns its tests.

```text
pyproject.toml                  # [tool.uv.workspace] members = ["apps/*", "packages/*"]
uv.lock
README.md
AI_AGENT_SWARM_SPEC.md

apps/
├── api/                        # FastAPI: workflows, approvals, webhooks
│   └── src/swarm_api/
├── orchestrator/               # LangGraph runner + results and integration consumers
│   └── src/swarm_orchestrator/
├── worker/                     # generic agent worker; AGENT_TYPE selects the agent
│   └── src/swarm_worker/
└── outbox_worker/
    └── src/swarm_outbox/

packages/
├── contracts/                  # Pydantic models from §17
├── messaging/                  # MessageBus protocol + aiokafka/Redpanda adapter
├── database/                   # SQLAlchemy models, repositories, Alembic migrations
├── orchestration/              # graphs: jira_to_pr.py, pr_review.py, pr_command.py
├── agents/
│   ├── core/                   # Agent protocol, executor, retry, idempotency
│   ├── planner/
│   ├── researcher/
│   ├── coder/
│   ├── reviewer/
│   └── tester/
├── tools/                      # Tool protocol, registry, filesystem/shell/git tools
├── integrations/
│   ├── github/                 # GitHubClient, githubkit impl, tools, webhook normalizer
│   └── jira/                   # JiraClient, httpx impl, ADF, tools, webhook normalizer
├── workspaces/                 # WorkspaceManager: clone, mirror cache, cleanup
├── llm/                        # LlmProvider and implementations
├── observability/              # structlog, OpenTelemetry setup, metrics
└── shared/                     # settings, ids, time, errors

config/
├── agents.yaml
└── trigger_rules.yaml

docker/
├── compose.yaml                # redpanda, redpanda-console, postgres, redis
└── redpanda/
    ├── topics.yaml             # topics, partitions, retention
    └── bootstrap.sh            # rpk topic create (kafka-topics.sh fallback)
```

## 18.1 Dependency Direction

```text
apps/*  ──►  packages/orchestration, agents, integrations
                    │
                    ▼
        packages/messaging, database, tools, llm, workspaces
                    │
                    ▼
            packages/contracts, shared, observability
```

`contracts` and `shared` depend on nothing internal. No package may import from `apps/`.

---

# 19. Implementation Phases

## Phase 1 — Infrastructure

Redpanda plus Console, PostgreSQL and Redis in `docker/compose.yaml`; topic bootstrap with
`rpk`; `uv` workspace; Alembic baseline; `structlog`; `pydantic-settings`.

**Done when:** `docker compose up` gives a working local stack and `rpk topic list` shows
the six topics.

## Phase 2 — Messaging

`MessageBus` protocol, Redpanda adapter, Pydantic contracts, serialization, consumer groups,
idempotent producer, manual commits, cooperative rebalance.

**Done when:** a round-trip integration test publishes and consumes a task against a
`RedpandaContainer`.

## Phase 3 — Agent Runtime

`Agent`, `AgentWorker`, executor, retry with backoff, idempotency via `task_executions`,
DLQ, timeouts, backpressure, graceful shutdown.

**Done when:** a worker killed mid-task recovers without duplicating side effects.

## Phase 4 — LangGraph

`SwarmState`, graph builder, dispatch node with `interrupt()`, results consumer with
`Command(resume=...)`, `AsyncPostgresSaver`, capability routing.

**Done when:** a two-agent sequential workflow completes end to end through the broker.

## Phase 5 — Persistence

All tables from §14.1, repositories, outbox publisher process, human approvals.

**Done when:** a run's full history is reconstructible from PostgreSQL alone.

## Phase 6 — Integrations

`GitHubClient` with githubkit, `JiraClient` with httpx and ADF conversion, both tool sets,
both webhook endpoints and normalizers, `external_references`, `webhook_deliveries`,
trigger rules, `WorkspaceManager`.

**Done when:** a PR is opened on a real repository by an agent, and a Jira issue is
transitioned, both idempotently.

## Phase 7 — Automation Workflows

`jira-to-pr:v1`, `pr-review:v1`, `pr-command:v1`; approval gates; bounded review loop.

**Done when:** the MVP Definition of Done in §1.3 passes.

## Phase 8 — Parallelism

`Send` fan-out, reducers, fan-in with expected-ID tracking, aggregation, loop caps.

**Done when:** researcher and coder run concurrently and their results merge deterministically.

## Phase 9 — Production

OpenTelemetry traces and metrics, Redis rate limiters, security policies, sandboxed
workspaces, deployment manifests, autoscaling on consumer-group lag.

**Done when:** the platform runs unattended for a week with dashboards and alerting.

## 19.1 Deliberately Deferred

- dynamic agent marketplace and autonomous discovery;
- self-modifying workflows;
- distributed vector database;
- multi-region replication;
- automatic model selection;
- reinforcement learning;
- unrestricted agent-to-agent communication;
- automatic PR merging;
- GitLab, Bitbucket and Linear adapters — the adapter protocols are designed so they can be
  added without touching the core;
- Jira OAuth 3LO and marketplace distribution.

---

## Appendix A — Guiding Architecture

```text
                  ┌───────────────────────┐
                  │       LangGraph       │   "What happens next?"
                  └───────────┬───────────┘
                              ▼
                  ┌───────────────────────┐
                  │       Redpanda        │   "Move the work"
                  └───────────┬───────────┘
             ┌────────────────┼────────────────┐
             ▼                ▼                ▼
          Research          Coding           Review
           Worker           Worker           Worker
             │                │                │
             │         ┌──────┴──────┐  ┌──────┴──────┐
             │         │ GitHub/git  │  │ GitHub/Jira │  "Touch the outside world"
             │         └─────────────┘  └─────────────┘
             └────────────────┼────────────────┘
                              ▼
                        Result Events
                              ▼
                         LangGraph
                              ▼
                         PostgreSQL          "Remember it"
```

## Appendix B — End-to-End Example

Jira issue `PROJ-42`, "Implement OAuth authentication", labelled `ai-agent`, moved to
**Ready for Dev**.

| # | What happens |
|---|--------------|
| 1 | Jira webhook hits `/webhooks/jira`, is verified and deduplicated, lands on `integration.jira.events` |
| 2 | Orchestrator matches rule `jira-ready-for-dev`, creates `RUN-001` of `jira-to-pr:v1`, records the external reference |
| 3 | `fetch_issue` reads description, acceptance criteria and the target repo |
| 4 | `planner` produces the plan and the branch name `feature/PROJ-42-oauth-authentication` |
| 5 | `ensure_branch` creates it from `main`, or no-ops if it exists |
| 6 | `researcher` and `coder` run in parallel; the coder clones a workspace, implements, commits and pushes |
| 7 | `tester` runs the suite in a fresh workspace and reports |
| 8 | `open_draft_pr` opens `[PROJ-42] Implement OAuth authentication` as a draft, body carrying plan and test summary |
| 9 | `jira_update` adds the PR remote link and a comment, transitions the issue to *In Review* |
| 10 | `reviewer` reads the diff and posts a review; changes requested loops back to the coder, capped at 2 |
| 11 | `mark_ready` flips the PR out of draft; the run completes |
| 12 | A human merges. The resulting `pull_request.closed` event may trigger `post-merge:v1`, which transitions the issue to *Done* behind an approval gate |
