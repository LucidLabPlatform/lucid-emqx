# lucid-emqx

EMQX Enterprise 6.1.1 broker and idempotent provisioner for the LUCID IoT platform. Provides the MQTT message bus and enforces the full LUCID message contract via schema validation, message transformation, and a rule engine that writes all telemetry and state directly to Postgres — no HTTP connector involved.

---

## Repository contents

| File | Description |
|---|---|
| `Dockerfile` | Broker image — thin wrapper over `emqx/emqx-enterprise:6.1.1` |
| `Provisioner.Dockerfile` | Provisioner image — Python 3.13 slim, runs `setup_rules.py` |
| `setup_rules.py` | Idempotent provisioner: registers all Smart Data Hub and data-integration resources via the EMQX management API |
| `.env.example` | Reference environment file for both containers |
| `requirements.txt` | Provisioner dependency: `httpx>=0.27.0` |

---

## Two Docker images

### Broker (`Dockerfile`)

```dockerfile
FROM emqx/emqx-enterprise:6.1.1
```

Run the vanilla EMQX Enterprise image. All configuration is supplied through environment variables (see below). No files are baked in.

```bash
docker build -t lucid-emqx -f Dockerfile .
docker run -d \
  --name emqx \
  --env-file .env \
  -p 1883:1883 \
  -p 18083:18083 \
  lucid-emqx
```

### Provisioner (`Provisioner.Dockerfile`)

```dockerfile
FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY setup_rules.py ./
CMD ["python", "setup_rules.py"]
```

Run after the broker is healthy. The provisioner connects to the EMQX management API, creates or updates every resource, then exits.

```bash
docker build -t lucid-emqx-provisioner -f Provisioner.Dockerfile .
docker run --rm \
  --env-file .env \
  lucid-emqx-provisioner
```

The provisioner retries login up to 20 times (3-second intervals) to tolerate broker startup delay.

Verify the installed EMQX resources with:

```bash
python3 verify_resources.py \
  --url http://localhost:18083 \
  --username lucid \
  --password '<dashboard-password>' \
  --profile minimal
```

Use `--profile full` only when the full stack is running and EMQX is expected to have built-in-database auth and built-in-database authorization configured. LDAP is optional in this profile and will be reported when present.

---

## Environment variables

### Broker container

| Variable | Description | Default |
|---|---|---|
| `EMQX_NAME` | EMQX node name | `emqx` |
| `EMQX_HOST` | EMQX node host | `emqx` |
| `EMQX_DASHBOARD__DEFAULT_USERNAME` | Dashboard admin username | `lucid` |
| `EMQX_DASHBOARD__DEFAULT_PASSWORD` | Dashboard admin password | `REDACTED` |

### Provisioner container

| Variable | Description | Default |
|---|---|---|
| `EMQX_URL` | EMQX management API base URL | `http://localhost:18083` |
| `EMQX_USERNAME` | EMQX dashboard username | `admin` |
| `EMQX_PASSWORD` | EMQX dashboard password | `public` |
| `LUCID_DB_HOST` | Postgres host:port | `lucid-db:5432` |
| `LUCID_DB_NAME` | Postgres database name | `lucid` |
| `LUCID_DB_USER` | Postgres username | `lucid` |
| `LUCID_DB_PASSWORD` | Postgres password | `REDACTED` |

Copy `.env.example` to `.env` and change passwords before use.

---

## MQTT topic contract

All LUCID topics follow this strict tree. The provisioner enforces this via schema validations and rules — never invent new patterns.

```
lucid/agents/{agent_id}/
  metadata | status | state | cfg | cfg/logging | cfg/telemetry   ← retained, QoS 1
  logs | telemetry/{metric}                                        ← streams, QoS 0
  cmd/{action}                                                     ← commands (require request_id)
  evt/{action}/result                                              ← results, QoS 1
  components/{component_id}/...                                    ← same subtree per component
```

Every command payload must include `{"request_id": "<uuid>"}`. Every command produces a corresponding `evt/{action}/result` with `{request_id, ok, error}`.

Agent MQTT usernames and topic namespaces use the canonical `agent_id`. MQTT client IDs may include a presentation prefix such as `lucid.agent.<agent_id>`; the broker normalises those back to the canonical `agent_id` during ingestion.

---

## Provisioner execution order

The provisioner applies resources in this fixed order:

1. **Postgres connector** — establishes `lucid-postgres` before any actions reference it
2. **Schemas** — JSON Schema definitions registered in the Smart Data Hub schema registry
3. **Schema validations** — validators that drop non-conforming messages
4. **Message transformations** — decode/encode pipeline applied to all `lucid/#` messages
5. **Connectors** — additional connectors (currently none beyond the Postgres connector)
6. **Actions** — named Postgres INSERT/UPDATE statements
7. **Rules** — rule engine SQL that selects from MQTT topics and fans out to actions

The provisioner is idempotent. Re-running it is safe: existing resources are updated in place, not duplicated.

---

## Smart Data Hub

### Schemas (11)

JSON Schema definitions enforcing the LUCID message contract. All schemas are registered as type `json`.

| Schema name | Required fields | Purpose |
|---|---|---|
| `lucid-command` | `request_id` | All `cmd/` payloads |
| `lucid-agent-metadata` | `version`, `platform`, `architecture` | Agent metadata topic |
| `lucid-agent-status` | `state` | Agent status topic |
| `lucid-agent-state` | — | Agent state topic (cpu/memory/disk percents) |
| `lucid-agent-cfg` | `heartbeat_s` | Agent config topic |
| `lucid-agent-cfg-logging` | `level` | Agent and component logging config; enum: `debug\|info\|warning\|error` |
| `lucid-agent-cfg-telemetry` | — | Per-metric config objects (`enabled`, `interval_s`, `threshold`) |
| `lucid-telemetry` | `value` | All telemetry stream payloads |
| `lucid-event-result` | `request_id`, `ok` | All `evt/*/result` payloads |
| `lucid-component-metadata` | `version` | Component metadata topic |
| `lucid-component-status` | `state` | Component status topic |

### Validations (16)

All validations use `strategy: all_pass` and `failure_action: drop` — non-conforming messages are silently dropped (logged at warning level).

| Validation name | Topic pattern | Schema |
|---|---|---|
| `validate-agent-cmd` | `lucid/agents/+/cmd/#` | `lucid-command` |
| `validate-component-cmd` | `lucid/agents/+/components/+/cmd/#` | `lucid-command` |
| `validate-agent-metadata` | `lucid/agents/+/metadata` | `lucid-agent-metadata` |
| `validate-agent-status` | `lucid/agents/+/status` | `lucid-agent-status` |
| `validate-agent-state` | `lucid/agents/+/state` | `lucid-agent-state` |
| `validate-agent-cfg` | `lucid/agents/+/cfg` | `lucid-agent-cfg` |
| `validate-agent-cfg-logging` | `lucid/agents/+/cfg/logging` | `lucid-agent-cfg-logging` |
| `validate-agent-cfg-telemetry` | `lucid/agents/+/cfg/telemetry` | `lucid-agent-cfg-telemetry` |
| `validate-agent-telemetry` | `lucid/agents/+/telemetry/#` | `lucid-telemetry` |
| `validate-agent-events` | `lucid/agents/+/evt/#` | `lucid-event-result` |
| `validate-component-metadata` | `lucid/agents/+/components/+/metadata` | `lucid-component-metadata` |
| `validate-component-status` | `lucid/agents/+/components/+/status` | `lucid-component-status` |
| `validate-component-cfg-logging` | `lucid/agents/+/components/+/cfg/logging` | `lucid-agent-cfg-logging` |
| `validate-component-cfg-telemetry` | `lucid/agents/+/components/+/cfg/telemetry` | `lucid-agent-cfg-telemetry` |
| `validate-component-telemetry` | `lucid/agents/+/components/+/telemetry/#` | `lucid-telemetry` |
| `validate-component-events` | `lucid/agents/+/components/+/evt/#` | `lucid-event-result` |

### Message transformation (1)

| Name | Topics | Behaviour |
|---|---|---|
| `lucid-json-normalize` | `lucid/#` | Decode JSON payload on ingress, re-encode on egress. Messages that fail JSON decode are dropped. |

---

## Data integration

### Connector

One connector is provisioned: `lucid-postgres` (type `pgsql`), connecting to the `lucid-db` Postgres instance with a pool of 8 connections.

There is no HTTP connector. All data flows directly from the EMQX rule engine to Postgres.

### Actions

Actions are named Postgres statements reused across rules. All upserts use `ON CONFLICT DO UPDATE` to keep tables current.

**Shared FK-parent upserts** (run first in every rule that writes agent or component data)

| Action | Table | Behaviour |
|---|---|---|
| `upsert-agents` | `agents` | Insert on first seen, update `last_seen_ts` |
| `upsert-components` | `components` | Insert on first seen, update `last_seen_ts` |

**Agent retained**

| Action | Table |
|---|---|
| `agent-metadata-sink` | `agent_metadata` |
| `agent-status-sink` | `agent_status` |
| `agent-state-sink` | `agent_state` |
| `agent-cfg-sink` | `agent_cfg` |
| `agent-cfg-logging-sink` | `agent_cfg_logging` |
| `agent-cfg-telemetry-sink` | `agent_cfg_telemetry` |

**Agent streaming**

| Action | Table | Notes |
|---|---|---|
| `agent-logs-sink` | `logs` | `component_id` is NULL for agent-level logs |
| `agent-telemetry-sink` | `agent_telemetry` | `metric` extracted from topic suffix |
| `agent-events-sink` | `agent_events` | Records `evt/#` results |
| `agent-commands-backfill` | `commands` | Updates `result_ok`/`result_ts` where `request_id` matches and result not yet set |

**Component retained**

| Action | Table |
|---|---|
| `component-metadata-sink` | `component_metadata` |
| `component-status-sink` | `component_status` |
| `component-state-sink` | `component_state` — raw JSONB payload |
| `component-cfg-sink` | `component_cfg` — raw JSONB payload |
| `component-cfg-logging-sink` | `component_cfg_logging` |
| `component-cfg-telemetry-sink` | `component_cfg_telemetry` — raw JSONB payload |

**Component streaming**

| Action | Table | Notes |
|---|---|---|
| `component-logs-sink` | `logs` | `component_id` populated |
| `component-telemetry-sink` | `component_telemetry` | `metric` extracted from topic suffix |
| `component-events-sink` | `component_events` | Includes `applied` field |
| `component-commands-backfill` | `commands` | Same pattern as agent backfill |

**Client events**

| Action | Table |
|---|---|
| `client-events-sink` | `client_events` — connect/disconnect events |

### Rules (19)

Rules select from MQTT topics, transform fields with the EMQX rule engine SQL dialect, and fan out to one or more actions. `agent_id` is always extracted from the MQTT `clientid` (not parsed from the topic string). `component_id` is extracted via `substr` + `split` from the topic path.

**Agent retained (6)**

| Rule description | Topic | Actions |
|---|---|---|
| `lucid:agent-metadata` | `lucid/agents/+/metadata` | upsert-agents, agent-metadata-sink |
| `lucid:agent-status` | `lucid/agents/+/status` | upsert-agents, agent-status-sink |
| `lucid:agent-state` | `lucid/agents/+/state` | upsert-agents, agent-state-sink |
| `lucid:agent-cfg` | `lucid/agents/+/cfg` | upsert-agents, agent-cfg-sink |
| `lucid:agent-cfg-logging` | `lucid/agents/+/cfg/logging` | upsert-agents, agent-cfg-logging-sink |
| `lucid:agent-cfg-telemetry` | `lucid/agents/+/cfg/telemetry` | upsert-agents, agent-cfg-telemetry-sink |

**Agent streaming (3)**

| Rule description | Topic | Actions |
|---|---|---|
| `lucid:agent-logs` | `lucid/agents/+/logs` | upsert-agents, agent-logs-sink |
| `lucid:agent-telemetry` | `lucid/agents/+/telemetry/#` | upsert-agents, agent-telemetry-sink |
| `lucid:agent-events` | `lucid/agents/+/evt/#` | upsert-agents, agent-events-sink, agent-commands-backfill |

**Component retained (6)**

| Rule description | Topic | Actions |
|---|---|---|
| `lucid:component-metadata` | `lucid/agents/+/components/+/metadata` | upsert-agents, upsert-components, component-metadata-sink |
| `lucid:component-status` | `lucid/agents/+/components/+/status` | upsert-agents, upsert-components, component-status-sink |
| `lucid:component-state` | `lucid/agents/+/components/+/state` | upsert-agents, upsert-components, component-state-sink |
| `lucid:component-cfg` | `lucid/agents/+/components/+/cfg` | upsert-agents, upsert-components, component-cfg-sink |
| `lucid:component-cfg-logging` | `lucid/agents/+/components/+/cfg/logging` | upsert-agents, upsert-components, component-cfg-logging-sink |
| `lucid:component-cfg-telemetry` | `lucid/agents/+/components/+/cfg/telemetry` | upsert-agents, upsert-components, component-cfg-telemetry-sink |

**Component streaming (3)**

| Rule description | Topic | Actions |
|---|---|---|
| `lucid:component-logs` | `lucid/agents/+/components/+/logs` | upsert-agents, upsert-components, component-logs-sink |
| `lucid:component-telemetry` | `lucid/agents/+/components/+/telemetry/#` | upsert-agents, upsert-components, component-telemetry-sink |
| `lucid:component-events` | `lucid/agents/+/components/+/evt/#` | upsert-agents, upsert-components, component-events-sink, component-commands-backfill |

**Client events (1)**

| Rule description | Topic | Actions |
|---|---|---|
| `lucid:client-events` | `$events/client_connected`, `$events/client_disconnected` | client-events-sink |

---

## Idempotency

The provisioner is safe to re-run at any time:

- **Schemas**: PUT (update) if name exists, POST (create) otherwise.
- **Validations**: matched by name, PUT if found.
- **Transformations**: matched by name, PUT if found.
- **Connectors**: skipped if already present (connector config is stable).
- **Actions**: always PUT (update) if found — SQL fixes are applied on re-run.
- **Rules**: matched by `description` field, PUT if found — SQL fixes are applied on re-run.

Exit code is 0 on success, non-zero on any API error.
