#!/usr/bin/env python3
"""
EMQX Rule Engine provisioning for LUCID central command.

Creates all Smart Data Hub resources (schemas, validations, transformations)
and data-integration resources (connectors, actions, rules) that enforce the
LUCID message contract and move every MQTT DB write into EMQX directly.

Idempotent — safe to re-run at any time.

Usage:
    python3 services/emqx/setup_rules.py

Environment variables (all have sensible defaults):
    EMQX_URL          EMQX management API base URL  (default: http://localhost:18083)
    EMQX_USERNAME     EMQX dashboard username        (default: lucid)
    EMQX_PASSWORD     EMQX dashboard password        (default: REDACTED)
    LUCID_DB_HOST     Postgres host:port             (default: lucid-db:5432)
    LUCID_DB_NAME         Postgres database name         (default: lucid)
    LUCID_DB_USER         Postgres username              (default: lucid)
    LUCID_DB_PASSWORD     Postgres password              (default: REDACTED)
"""

import json
import os
import sys
import time

import httpx

EMQX_URL      = os.environ.get("EMQX_URL",      "http://localhost:18083")
EMQX_USERNAME = os.environ.get("EMQX_USERNAME", "admin")
EMQX_PASSWORD = os.environ.get("EMQX_PASSWORD", "public")
LUCID_DB_HOST     = os.environ.get("LUCID_DB_HOST",     "lucid-db:5432")
LUCID_DB_NAME     = os.environ.get("LUCID_DB_NAME",     "lucid")
LUCID_DB_USER     = os.environ.get("LUCID_DB_USER",     "lucid")
LUCID_DB_PASSWORD = os.environ.get("LUCID_DB_PASSWORD", "REDACTED")


# ---------------------------------------------------------------------------
# Thin EMQX REST client
# ---------------------------------------------------------------------------

class EMQXClient:
    def __init__(self) -> None:
        self._base = EMQX_URL
        self._token = self._login()

    def _login(self) -> str:
        last_error = None
        for _ in range(20):
            try:
                resp = httpx.post(
                    f"{self._base}/api/v5/login",
                    json={"username": EMQX_USERNAME, "password": EMQX_PASSWORD},
                    timeout=30,
                )
                resp.raise_for_status()
                return resp.json()["token"]
            except Exception as exc:
                last_error = exc
                time.sleep(3)
        raise RuntimeError(f"Could not log in to EMQX: {last_error}")

    def _h(self) -> dict:
        return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}

    def get(self, path: str) -> httpx.Response:
        return httpx.get(f"{self._base}{path}", headers=self._h(), timeout=30)

    def post(self, path: str, body: dict) -> httpx.Response:
        return httpx.post(f"{self._base}{path}", json=body, headers=self._h(), timeout=30)

    def put(self, path: str, body: dict) -> httpx.Response:
        return httpx.put(f"{self._base}{path}", json=body, headers=self._h(), timeout=30)

    def delete(self, path: str) -> httpx.Response:
        return httpx.delete(f"{self._base}{path}", headers=self._h(), timeout=30)


# ---------------------------------------------------------------------------
# Idempotent resource helpers
# ---------------------------------------------------------------------------

def ensure_connector(client: EMQXClient, spec: dict) -> None:
    name      = spec["name"]
    type_name = spec["type"]
    resp = client.get(f"/api/v5/connectors/{type_name}:{name}")
    if resp.status_code == 200:
        print(f"  [skip]   connector {name}")
        return
    resp = client.post("/api/v5/connectors", spec)
    if resp.status_code not in (200, 201):
        print(f"  [ERROR]  connector {name}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    status = resp.json().get("status", "?")
    print(f"  [create] connector {name} ({status})")


def ensure_action(client: EMQXClient, spec: dict) -> None:
    name      = spec["name"]
    type_name = spec["type"]
    resp = client.get(f"/api/v5/actions/{type_name}:{name}")
    if resp.status_code == 200:
        update_body = {k: v for k, v in spec.items() if k not in ("name", "type")}
        upd = client.put(f"/api/v5/actions/{type_name}:{name}", update_body)
        if upd.status_code not in (200, 201):
            print(f"  [ERROR]  action update {name}: {upd.text}", file=sys.stderr)
            sys.exit(1)
        print(f"  [update] action {name}")
        return
    resp = client.post("/api/v5/actions", spec)
    if resp.status_code not in (200, 201):
        print(f"  [ERROR]  action {name}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    status = resp.json().get("status", "?")
    print(f"  [create] action {name} ({status})")


def ensure_rule(client: EMQXClient, spec: dict) -> None:
    """Create or update a rule by description (idempotent)."""
    desc = spec.get("description", "")
    resp = client.get("/api/v5/rules?limit=500")
    resp.raise_for_status()
    existing_id = None
    for rule in resp.json().get("data", []):
        if rule.get("description") == desc:
            existing_id = rule["id"]
            break
    if existing_id:
        # Update the existing rule SQL so fixes are applied on re-run
        upd = client.put(f"/api/v5/rules/{existing_id}", spec)
        if upd.status_code not in (200, 201):
            print(f"  [ERROR]  rule update '{desc}': {upd.text}", file=sys.stderr)
            sys.exit(1)
        print(f"  [update] rule '{desc}' (id={existing_id})")
        return
    resp = client.post("/api/v5/rules", spec)
    if resp.status_code not in (200, 201):
        print(f"  [ERROR]  rule '{desc}': {resp.text}", file=sys.stderr)
        sys.exit(1)
    print(f"  [create] rule '{desc}' (id={resp.json().get('id', '?')})")


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def _agent_id_from_topic(_prefix: str) -> str:
    """
    Extract agent_id from the MQTT topic path.

    Agent MQTT usernames and topic namespaces use the canonical agent_id.
    MQTT client IDs may include a presentation prefix such as
    ``lucid.agent.<agent_id>`` and should not be treated as canonical fleet
    identity.
    """
    return "nth(3, split(topic, '/')) as agent_id"


def _component_ids_from_topic(_prefix: str) -> str:
    """
    Extract agent_id and component_id from component topics.

    Topic pattern: lucid/agents/{agent_id}/components/{component_id}/...
    Canonical agent identity comes from the topic path, not the MQTT client ID.
    """
    return (
        "nth(3, split(topic, '/')) as agent_id, "
        "nth(5, split(topic, '/')) as component_id"
    )


def _normalized_agent_id_from_client() -> str:
    """Normalize clientid-based broker events to the canonical agent_id."""
    return (
        "CASE "
        "WHEN substr(clientid, 1, 12) = 'lucid.agent.' THEN substr(clientid, 13) "
        "ELSE clientid "
        "END as agent_id"
    )


def _agent_metric_from_topic() -> str:
    return "substr(topic, strlen(nth(3, split(topic, '/'))) + 24) as metric"


def _agent_event_action_from_topic() -> str:
    return "coalesce(payload.action, substr(topic, strlen(nth(3, split(topic, '/'))) + 18)) as action"


def _component_metric_from_topic() -> str:
    return (
        "substr(topic, "
        "strlen(nth(3, split(topic, '/'))) + strlen(nth(5, split(topic, '/'))) + 36"
        ") as metric"
    )


def _component_event_action_from_topic() -> str:
    return (
        "coalesce(payload.action, substr(topic, "
        "strlen(nth(3, split(topic, '/'))) + strlen(nth(5, split(topic, '/'))) + 30"
        ")) as action"
    )


def _agent_id_from_command_topic() -> str:
    """Extract agent_id from a command topic path instead of publisher clientid."""
    return "nth(3, split(topic, '/')) as agent_id"


def _component_ids_from_command_topic() -> str:
    """Extract agent_id and component_id from component command topic paths."""
    return (
        "nth(3, split(topic, '/')) as agent_id, "
        "nth(5, split(topic, '/')) as component_id"
    )


def _agent_command_action_from_topic() -> str:
    """
    Extract the full action suffix from:
    lucid/agents/{agent_id}/cmd/{action...}
    """
    return "substr(topic, strlen(nth(3, split(topic, '/'))) + 18) as action"


def _component_command_action_from_topic() -> str:
    """
    Extract the full action suffix from:
    lucid/agents/{agent_id}/components/{component_id}/cmd/{action...}
    """
    return (
        "substr(topic, "
        "strlen(nth(3, split(topic, '/'))) + strlen(nth(5, split(topic, '/'))) + 30"
        ") as action"
    )


UPSERT_AGENTS_SQL = """
UPDATE mqtt_users
SET last_seen_ts  = ${received_ts}::text::timestamptz,
    first_seen_ts = COALESCE(first_seen_ts, ${received_ts}::text::timestamptz)
WHERE username = ${agent_id} AND role = 'agent'
""".strip()

UPSERT_COMPONENTS_SQL = """
INSERT INTO components (agent_id, component_id, first_seen_ts, last_seen_ts)
VALUES (${agent_id}, ${component_id}, ${received_ts}::text::timestamptz, ${received_ts}::text::timestamptz)
ON CONFLICT (agent_id, component_id) DO UPDATE SET last_seen_ts = EXCLUDED.last_seen_ts
""".strip()


def _pgsql_action(name: str, sql: str) -> dict:
    return {
        "type": "pgsql",
        "name": name,
        "connector": "lucid-postgres",
        "parameters": {"sql": sql.strip()},
    }


# ---------------------------------------------------------------------------
# Resource definitions
# ---------------------------------------------------------------------------

def connectors() -> list[dict]:
    return []


def actions() -> list[dict]:
    return [
        # ── shared FK-parent upserts ─────────────────────────────────────
        _pgsql_action("upsert-agents", UPSERT_AGENTS_SQL),
        _pgsql_action("upsert-components", UPSERT_COMPONENTS_SQL),

        # ── agent retained ───────────────────────────────────────────────
        _pgsql_action("agent-metadata-sink", """
            INSERT INTO agent_metadata (agent_id, version, platform, architecture, ip_address, received_ts)
            VALUES (${agent_id}, ${version}, ${platform}, ${architecture}, ${ip_address}, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id) DO UPDATE SET
                version=EXCLUDED.version, platform=EXCLUDED.platform,
                architecture=EXCLUDED.architecture, ip_address=EXCLUDED.ip_address,
                received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("agent-status-sink", """
            INSERT INTO agent_status (agent_id, state, connected_since_ts, uptime_s, received_ts)
            VALUES (${agent_id}, ${state}, ${connected_since_ts}::text::timestamptz, ${uptime_s}::text::float8,
                    ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id) DO UPDATE SET
                state=EXCLUDED.state, connected_since_ts=EXCLUDED.connected_since_ts,
                uptime_s=EXCLUDED.uptime_s,
                received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("agent-state-sink", """
            INSERT INTO agent_state (agent_id, components, received_ts)
            VALUES (${agent_id}, ${components}::jsonb, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id) DO UPDATE SET
                components=EXCLUDED.components,
                received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("agent-cfg-sink", """
            INSERT INTO agent_cfg (agent_id, heartbeat_s, received_ts)
            VALUES (${agent_id}, ${heartbeat_s}::text::integer, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id) DO UPDATE SET
                heartbeat_s=EXCLUDED.heartbeat_s, received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("agent-cfg-logging-sink", """
            INSERT INTO agent_cfg_logging (agent_id, log_level, received_ts)
            VALUES (${agent_id}, ${log_level}, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id) DO UPDATE SET
                log_level=EXCLUDED.log_level, received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("agent-cfg-telemetry-sink", """
            INSERT INTO agent_cfg_telemetry (
                agent_id,
                cpu_pct_enabled, cpu_pct_interval_s, cpu_pct_threshold,
                memory_pct_enabled, memory_pct_interval_s, memory_pct_threshold,
                disk_pct_enabled, disk_pct_interval_s, disk_pct_threshold,
                received_ts
            ) VALUES (
                ${agent_id},
                ${cpu_pct_enabled}::text::boolean, ${cpu_pct_interval_s}::text::integer, ${cpu_pct_threshold}::text::float8,
                ${memory_pct_enabled}::text::boolean, ${memory_pct_interval_s}::text::integer, ${memory_pct_threshold}::text::float8,
                ${disk_pct_enabled}::text::boolean, ${disk_pct_interval_s}::text::integer, ${disk_pct_threshold}::text::float8,
                ${received_ts}::text::timestamptz
            )
            ON CONFLICT (agent_id) DO UPDATE SET
                cpu_pct_enabled=EXCLUDED.cpu_pct_enabled,
                cpu_pct_interval_s=EXCLUDED.cpu_pct_interval_s,
                cpu_pct_threshold=EXCLUDED.cpu_pct_threshold,
                memory_pct_enabled=EXCLUDED.memory_pct_enabled,
                memory_pct_interval_s=EXCLUDED.memory_pct_interval_s,
                memory_pct_threshold=EXCLUDED.memory_pct_threshold,
                disk_pct_enabled=EXCLUDED.disk_pct_enabled,
                disk_pct_interval_s=EXCLUDED.disk_pct_interval_s,
                disk_pct_threshold=EXCLUDED.disk_pct_threshold,
                received_ts=EXCLUDED.received_ts
        """),

        _pgsql_action("agent-schema-sink", """
            INSERT INTO agent_schema (agent_id, publishes, subscribes, received_ts)
            VALUES (${agent_id}, ${publishes}::jsonb, ${subscribes}::jsonb, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id) DO UPDATE SET
                publishes=EXCLUDED.publishes,
                subscribes=EXCLUDED.subscribes,
                received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("component-schema-sink", """
            INSERT INTO component_schema (agent_id, component_id, publishes, subscribes, received_ts)
            VALUES (${agent_id}, ${component_id}, ${publishes}::jsonb, ${subscribes}::jsonb, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id, component_id) DO UPDATE SET
                publishes=EXCLUDED.publishes,
                subscribes=EXCLUDED.subscribes,
                received_ts=EXCLUDED.received_ts
        """),

        # ── agent streaming ──────────────────────────────────────────────
        _pgsql_action("agent-logs-sink", """
            INSERT INTO logs (agent_id, component_id, payload, received_ts)
            VALUES (${agent_id}, NULL, ${log_payload}, ${received_ts}::text::timestamptz)
        """),
        _pgsql_action("agent-commands-sink", """
            INSERT INTO commands (
                request_id, agent_id, component_id, action, topic, payload,
                publisher_username, publisher_clientid, result_received, sent_ts
            )
            VALUES (
                ${request_id}, ${agent_id}, NULL, ${action}, ${topic}, ${command_payload},
                ${publisher_username}, ${publisher_clientid}, false, ${sent_ts}::text::timestamptz
            )
            ON CONFLICT (request_id) DO NOTHING
        """),
        _pgsql_action("agent-telemetry-sink", """
            INSERT INTO agent_telemetry (agent_id, metric, value, received_ts)
            VALUES (${agent_id}, ${metric}, ${value}::text::float8, ${received_ts}::text::timestamptz)
        """),
        _pgsql_action("agent-events-sink", """
            INSERT INTO agent_events (agent_id, action, request_id, ok, error, applied, received_ts)
            VALUES (${agent_id}, ${action}, ${request_id}, ${ok}::text::boolean,
                    ${error}, ${applied}::jsonb, ${received_ts}::text::timestamptz)
        """),
        _pgsql_action("agent-commands-backfill", """
            UPDATE commands
            SET result_received=true,
                result_ok=${ok}::text::boolean,
                result_ts=${received_ts}::text::timestamptz
            WHERE request_id=${request_id} AND result_received = false AND ${request_id} != ''
        """),

        # ── component retained ───────────────────────────────────────────
        _pgsql_action("component-metadata-sink", """
            INSERT INTO component_metadata (agent_id, component_id, version, capabilities, received_ts)
            VALUES (${agent_id}, ${component_id}, ${version}, ${capabilities}::jsonb, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id, component_id) DO UPDATE SET
                version=EXCLUDED.version, capabilities=EXCLUDED.capabilities,
                received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("component-status-sink", """
            INSERT INTO component_status (agent_id, component_id, state, received_ts)
            VALUES (${agent_id}, ${component_id}, ${state}, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id, component_id) DO UPDATE SET
                state=EXCLUDED.state, received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("component-state-sink", """
            INSERT INTO component_state (agent_id, component_id, payload, received_ts)
            VALUES (${agent_id}, ${component_id}, ${comp_payload}, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id, component_id) DO UPDATE SET
                payload=EXCLUDED.payload, received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("component-cfg-sink", """
            INSERT INTO component_cfg (agent_id, component_id, payload, received_ts)
            VALUES (${agent_id}, ${component_id}, ${comp_payload}, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id, component_id) DO UPDATE SET
                payload=EXCLUDED.payload, received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("component-cfg-logging-sink", """
            INSERT INTO component_cfg_logging (agent_id, component_id, log_level, received_ts)
            VALUES (${agent_id}, ${component_id}, ${log_level}, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id, component_id) DO UPDATE SET
                log_level=EXCLUDED.log_level, received_ts=EXCLUDED.received_ts
        """),
        _pgsql_action("component-cfg-telemetry-sink", """
            INSERT INTO component_cfg_telemetry (agent_id, component_id, payload, received_ts)
            VALUES (${agent_id}, ${component_id}, ${comp_payload}, ${received_ts}::text::timestamptz)
            ON CONFLICT (agent_id, component_id) DO UPDATE SET
                payload=EXCLUDED.payload, received_ts=EXCLUDED.received_ts
        """),

        # ── component streaming ──────────────────────────────────────────
        _pgsql_action("component-logs-sink", """
            INSERT INTO logs (agent_id, component_id, payload, received_ts)
            VALUES (${agent_id}, ${component_id}, ${log_payload}, ${received_ts}::text::timestamptz)
        """),
        _pgsql_action("component-commands-sink", """
            INSERT INTO commands (
                request_id, agent_id, component_id, action, topic, payload,
                publisher_username, publisher_clientid, result_received, sent_ts
            )
            VALUES (
                ${request_id}, ${agent_id}, ${component_id}, ${action}, ${topic}, ${command_payload},
                ${publisher_username}, ${publisher_clientid}, false, ${sent_ts}::text::timestamptz
            )
            ON CONFLICT (request_id) DO NOTHING
        """),
        _pgsql_action("component-telemetry-sink", """
            INSERT INTO component_telemetry (agent_id, component_id, metric, value, received_ts)
            VALUES (${agent_id}, ${component_id}, ${metric}, ${value_json}::jsonb,
                    ${received_ts}::text::timestamptz)
        """),
        _pgsql_action("component-events-sink", """
            INSERT INTO component_events
                (agent_id, component_id, action, request_id, ok, applied, error, received_ts)
            VALUES (${agent_id}, ${component_id}, ${action}, ${request_id},
                    ${ok}::text::boolean, ${applied}::jsonb, ${error}, ${received_ts}::text::timestamptz)
        """),
        _pgsql_action("component-commands-backfill", """
            UPDATE commands
            SET result_received=true,
                result_ok=${ok}::text::boolean,
                result_ts=${received_ts}::text::timestamptz
            WHERE request_id=${request_id} AND result_received = false AND ${request_id} != ''
        """),

        # ── client events ────────────────────────────────────────────────
        _pgsql_action("client-events-sink", """
            INSERT INTO client_events (agent_id, event_type, ts)
            VALUES (${agent_id}, ${event_type}, ${ts}::text::timestamptz)
        """),
        _pgsql_action("authn-log-sink", """
            INSERT INTO authn_log (ts, username, clientid, result)
            VALUES (${ts}::text::timestamptz, ${username}, ${clientid}, ${result})
        """),
        _pgsql_action("authn-denied-sink", """
            INSERT INTO authn_denied (ts, username, clientid, result)
            VALUES (${ts}::text::timestamptz, ${username}, ${clientid}, ${result})
        """),
        _pgsql_action("authz-log-sink", """
            INSERT INTO authz_log (ts, username, clientid, topic, action, result)
            VALUES (${ts}::text::timestamptz, ${username}, ${clientid}, ${topic}, ${action}, ${result})
        """),
        _pgsql_action("authz-denied-sink", """
            INSERT INTO authz_denied (ts, username, clientid, topic, action, result)
            VALUES (${ts}::text::timestamptz, ${username}, ${clientid}, ${topic}, ${action}, ${result})
        """),

        # ── rejected messages ────────────────────────────────────────────
        _pgsql_action("rejected-messages-sink", """
            INSERT INTO mqtt_rejected_messages (topic, agent_id, raw_payload, validation_name, received_ts)
            VALUES (${topic}, ${agent_id}, ${raw_payload}, ${validation_name}, ${received_ts}::text::timestamptz)
        """),
    ]


# (description, topic_pattern, schema_name) — one entry per schema validation.
# Each generates a rejection rule: fires when NOT schema_check() passes.
_REJECTION_RULES: list[tuple[str, str, str]] = [
    # commands
    ("lucid:reject:agent-cmd",               "lucid/agents/+/cmd/#",                        "lucid-command"),
    ("lucid:reject:component-cmd",           "lucid/agents/+/components/+/cmd/#",           "lucid-command"),
    # agent retained
    ("lucid:reject:agent-metadata",          "lucid/agents/+/metadata",                     "lucid-agent-metadata"),
    ("lucid:reject:agent-status",            "lucid/agents/+/status",                       "lucid-agent-status"),
    ("lucid:reject:agent-state",             "lucid/agents/+/state",                        "lucid-agent-state"),
    ("lucid:reject:agent-cfg",               "lucid/agents/+/cfg",                          "lucid-agent-cfg"),
    ("lucid:reject:agent-cfg-logging",       "lucid/agents/+/cfg/logging",                  "lucid-agent-cfg-logging"),
    ("lucid:reject:agent-cfg-telemetry",     "lucid/agents/+/cfg/telemetry",                "lucid-agent-cfg-telemetry"),
    ("lucid:reject:agent-schema",            "lucid/agents/+/schema",                       "lucid-schema-topic"),
    ("lucid:reject:component-schema",        "lucid/agents/+/components/+/schema",          "lucid-schema-topic"),
    # agent streaming
    ("lucid:reject:agent-telemetry",         "lucid/agents/+/telemetry/#",                  "lucid-telemetry"),
    ("lucid:reject:agent-events",            "lucid/agents/+/evt/#",                        "lucid-event-result"),
    # component retained
    ("lucid:reject:component-metadata",      "lucid/agents/+/components/+/metadata",        "lucid-component-metadata"),
    ("lucid:reject:component-status",        "lucid/agents/+/components/+/status",          "lucid-component-status"),
    ("lucid:reject:component-cfg-logging",   "lucid/agents/+/components/+/cfg/logging",     "lucid-agent-cfg-logging"),
    ("lucid:reject:component-cfg-telemetry", "lucid/agents/+/components/+/cfg/telemetry",   "lucid-agent-cfg-telemetry"),
    # component streaming
    ("lucid:reject:component-telemetry",     "lucid/agents/+/components/+/telemetry/#",     "lucid-telemetry"),
    ("lucid:reject:component-events",        "lucid/agents/+/components/+/evt/#",           "lucid-event-result"),
]


def _rejection_sql(topic_pattern: str, schema_name: str) -> str:
    return f"""
        SELECT
            topic,
            {_normalized_agent_id_from_client()},
            json_encode(payload) as raw_payload,
            '{schema_name}' as validation_name,
            now_rfc3339() as received_ts
        FROM "{topic_pattern}"
        WHERE NOT schema_check('{schema_name}', payload)
    """


def rules() -> list[dict]:
    def rule(desc: str, sql: str, action_refs: list[str]) -> dict:
        return {
            "sql": sql.strip(),
            "actions": action_refs,
            "enable": True,
            "description": desc,
        }

    # ── agent-level rules ────────────────────────────────────────────────

    agent_metadata_sql = f"""
        SELECT
            {_agent_id_from_topic('metadata')},
            payload.version as version,
            payload.platform as platform,
            payload.architecture as architecture,
            payload.ip_address as ip_address,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/metadata"
        WHERE schema_check('lucid-agent-metadata', payload)
    """

    agent_status_sql = f"""
        SELECT
            {_agent_id_from_topic('status')},
            payload.state as state,
            payload.connected_since_ts as connected_since_ts,
            payload.uptime_s as uptime_s,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/status"
        WHERE schema_check('lucid-agent-status', payload)
    """

    agent_state_sql = f"""
        SELECT
            {_agent_id_from_topic('state')},
            json_encode(payload.components) as components,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/state"
        WHERE schema_check('lucid-agent-state', payload)
    """

    agent_cfg_sql = f"""
        SELECT
            {_agent_id_from_topic('cfg')},
            payload.heartbeat_s as heartbeat_s,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/cfg"
        WHERE schema_check('lucid-agent-cfg', payload)
    """

    agent_cfg_logging_sql = f"""
        SELECT
            {_agent_id_from_topic('cfg/logging')},
            payload.log_level as log_level,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/cfg/logging"
        WHERE schema_check('lucid-agent-cfg-logging', payload)
    """

    agent_cfg_telemetry_sql = f"""
        SELECT
            {_agent_id_from_topic('cfg/telemetry')},
            payload.cpu_percent.enabled as cpu_pct_enabled,
            payload.cpu_percent.interval_s as cpu_pct_interval_s,
            payload.cpu_percent.change_threshold_percent as cpu_pct_threshold,
            payload.memory_percent.enabled as memory_pct_enabled,
            payload.memory_percent.interval_s as memory_pct_interval_s,
            payload.memory_percent.change_threshold_percent as memory_pct_threshold,
            payload.disk_percent.enabled as disk_pct_enabled,
            payload.disk_percent.interval_s as disk_pct_interval_s,
            payload.disk_percent.change_threshold_percent as disk_pct_threshold,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/cfg/telemetry"
        WHERE schema_check('lucid-agent-cfg-telemetry', payload)
    """

    agent_schema_sql = f"""
        SELECT
            {_agent_id_from_topic('schema')},
            json_encode(payload.publishes) as publishes,
            json_encode(payload.subscribes) as subscribes,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/schema"
        WHERE schema_check('lucid-schema-topic', payload)
    """

    comp_schema_sql = f"""
        SELECT
            {_component_ids_from_topic('schema')},
            json_encode(payload.publishes) as publishes,
            json_encode(payload.subscribes) as subscribes,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/components/+/schema"
        WHERE schema_check('lucid-schema-topic', payload)
    """

    agent_logs_sql = f"""
        SELECT
            {_agent_id_from_topic('logs')},
            payload as log_payload,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/logs"
    """

    agent_commands_sql = f"""
        SELECT
            {_agent_id_from_command_topic()},
            {_agent_command_action_from_topic()},
            payload.request_id as request_id,
            topic as topic,
            payload as command_payload,
            username as publisher_username,
            clientid as publisher_clientid,
            now_rfc3339() as sent_ts
        FROM "lucid/agents/+/cmd/#"
        WHERE schema_check('lucid-command', payload)
    """

    agent_telemetry_sql = f"""
        SELECT
            {_agent_id_from_topic('telemetry')},
            {_agent_metric_from_topic()},
            payload.value as value,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/telemetry/#"
        WHERE is_not_null(payload.value)
          AND schema_check('lucid-telemetry', payload)
    """

    agent_events_sql = f"""
        SELECT
            {_agent_id_from_topic('evt')},
            {_agent_event_action_from_topic()},
            coalesce(payload.request_id, '') as request_id,
            coalesce(payload.ok, coalesce(payload.success, false)) as ok,
            payload.error as error,
            payload.applied as applied,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/evt/#"
        WHERE schema_check('lucid-event-result', payload)
    """

    # ── component-level rules ────────────────────────────────────────────

    comp_metadata_sql = f"""
        SELECT
            {_component_ids_from_topic('metadata')},
            payload.version as version,
            json_encode(payload.capabilities) as capabilities,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/components/+/metadata"
        WHERE schema_check('lucid-component-metadata', payload)
    """

    comp_status_sql = f"""
        SELECT
            {_component_ids_from_topic('status')},
            payload.state as state,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/components/+/status"
        WHERE schema_check('lucid-component-status', payload)
    """

    # component state/cfg/cfg_telemetry payloads are stored as raw JSONB
    comp_state_sql = f"""
        SELECT
            {_component_ids_from_topic('state')},
            payload as comp_payload,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/components/+/state"
    """

    comp_cfg_sql = f"""
        SELECT
            {_component_ids_from_topic('cfg')},
            payload as comp_payload,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/components/+/cfg"
    """

    comp_cfg_logging_sql = f"""
        SELECT
            {_component_ids_from_topic('cfg/logging')},
            payload.log_level as log_level,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/components/+/cfg/logging"
        WHERE schema_check('lucid-agent-cfg-logging', payload)
    """

    comp_cfg_telemetry_sql = f"""
        SELECT
            {_component_ids_from_topic('cfg/telemetry')},
            payload as comp_payload,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/components/+/cfg/telemetry"
        WHERE schema_check('lucid-agent-cfg-telemetry', payload)
    """

    comp_logs_sql = f"""
        SELECT
            {_component_ids_from_topic('logs')},
            payload as log_payload,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/components/+/logs"
    """

    comp_commands_sql = f"""
        SELECT
            {_component_ids_from_command_topic()},
            {_component_command_action_from_topic()},
            payload.request_id as request_id,
            topic as topic,
            payload as command_payload,
            username as publisher_username,
            clientid as publisher_clientid,
            now_rfc3339() as sent_ts
        FROM "lucid/agents/+/components/+/cmd/#"
        WHERE schema_check('lucid-command', payload)
    """

    comp_telemetry_sql = f"""
        SELECT
            {_component_ids_from_topic('telemetry')},
            {_component_metric_from_topic()},
            json_encode(payload.value) as value_json,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/components/+/telemetry/#"
        WHERE is_not_null(payload.value)
          AND schema_check('lucid-telemetry', payload)
    """

    comp_events_sql = f"""
        SELECT
            {_component_ids_from_topic('evt')},
            {_component_event_action_from_topic()},
            coalesce(payload.request_id, '') as request_id,
            coalesce(payload.ok, coalesce(payload.success, false)) as ok,
            payload.applied as applied,
            payload.error as error,
            now_rfc3339() as received_ts
        FROM "lucid/agents/+/components/+/evt/#"
        WHERE schema_check('lucid-event-result', payload)
    """

    client_events_sql = f"""
        SELECT
            {_normalized_agent_id_from_client()},
            event as event_type,
            now_rfc3339() as ts
        FROM "$events/client_connected", "$events/client_disconnected"
    """

    authn_log_sql = """
        SELECT
            coalesce(username, '') as username,
            clientid as clientid,
            coalesce(reason_code, 'unknown') as result,
            now_rfc3339() as ts
        FROM "$events/client_check_authn_complete"
    """

    authn_denied_sql = """
        SELECT
            coalesce(username, '') as username,
            clientid as clientid,
            coalesce(reason_code, 'unknown') as result,
            now_rfc3339() as ts
        FROM "$events/client_check_authn_complete"
        WHERE reason_code != 'success'
    """

    authz_log_sql = """
        SELECT
            coalesce(username, '') as username,
            clientid as clientid,
            topic as topic,
            action as action,
            coalesce(result, 'unknown') as result,
            now_rfc3339() as ts
        FROM "$events/client_check_authz_complete"
    """

    authz_denied_sql = """
        SELECT
            coalesce(username, '') as username,
            clientid as clientid,
            topic as topic,
            action as action,
            coalesce(result, 'unknown') as result,
            now_rfc3339() as ts
        FROM "$events/client_check_authz_complete"
        WHERE result = 'deny'
    """

    return [
        # agent retained
        rule("lucid:agent-metadata",      agent_metadata_sql,      ["pgsql:upsert-agents", "pgsql:agent-metadata-sink"]),
        rule("lucid:agent-status",        agent_status_sql,        ["pgsql:upsert-agents", "pgsql:agent-status-sink"]),
        rule("lucid:agent-state",         agent_state_sql,         ["pgsql:upsert-agents", "pgsql:agent-state-sink"]),
        rule("lucid:agent-cfg",           agent_cfg_sql,           ["pgsql:upsert-agents", "pgsql:agent-cfg-sink"]),
        rule("lucid:agent-cfg-logging",   agent_cfg_logging_sql,   ["pgsql:upsert-agents", "pgsql:agent-cfg-logging-sink"]),
        rule("lucid:agent-cfg-telemetry", agent_cfg_telemetry_sql, ["pgsql:upsert-agents", "pgsql:agent-cfg-telemetry-sink"]),
        rule("lucid:agent-schema",        agent_schema_sql,        ["pgsql:upsert-agents", "pgsql:agent-schema-sink"]),
        # agent streaming
        rule("lucid:agent-logs",          agent_logs_sql,          ["pgsql:upsert-agents", "pgsql:agent-logs-sink"]),
        rule("lucid:agent-commands",      agent_commands_sql,      ["pgsql:upsert-agents", "pgsql:agent-commands-sink"]),
        rule("lucid:agent-telemetry",     agent_telemetry_sql,     ["pgsql:upsert-agents", "pgsql:agent-telemetry-sink"]),
        rule("lucid:agent-events",        agent_events_sql,        ["pgsql:upsert-agents", "pgsql:agent-events-sink", "pgsql:agent-commands-backfill"]),
        # component retained
        rule("lucid:component-metadata",      comp_metadata_sql,      ["pgsql:upsert-agents", "pgsql:upsert-components", "pgsql:component-metadata-sink"]),
        rule("lucid:component-status",        comp_status_sql,        ["pgsql:upsert-agents", "pgsql:upsert-components", "pgsql:component-status-sink"]),
        rule("lucid:component-state",         comp_state_sql,         ["pgsql:upsert-agents", "pgsql:upsert-components", "pgsql:component-state-sink"]),
        rule("lucid:component-cfg",           comp_cfg_sql,           ["pgsql:upsert-agents", "pgsql:upsert-components", "pgsql:component-cfg-sink"]),
        rule("lucid:component-cfg-logging",   comp_cfg_logging_sql,   ["pgsql:upsert-agents", "pgsql:upsert-components", "pgsql:component-cfg-logging-sink"]),
        rule("lucid:component-cfg-telemetry", comp_cfg_telemetry_sql, ["pgsql:upsert-agents", "pgsql:upsert-components", "pgsql:component-cfg-telemetry-sink"]),
        rule("lucid:component-schema",        comp_schema_sql,        ["pgsql:upsert-agents", "pgsql:upsert-components", "pgsql:component-schema-sink"]),
        # component streaming — no upsert-components: only retained topics should create component records
        rule("lucid:component-logs",          comp_logs_sql,          ["pgsql:upsert-agents", "pgsql:component-logs-sink"]),
        rule("lucid:component-commands",      comp_commands_sql,      ["pgsql:upsert-agents", "pgsql:component-commands-sink"]),
        rule("lucid:component-telemetry",     comp_telemetry_sql,     ["pgsql:upsert-agents", "pgsql:component-telemetry-sink"]),
        rule("lucid:component-events",        comp_events_sql,        ["pgsql:upsert-agents", "pgsql:component-events-sink", "pgsql:component-commands-backfill"]),
        # client connect/disconnect
        rule("lucid:client-events", client_events_sql, ["pgsql:client-events-sink"]),
        rule("lucid:authn-log",    authn_log_sql,    ["pgsql:authn-log-sink"]),
        rule("lucid:authn-denied", authn_denied_sql, ["pgsql:authn-denied-sink"]),
        rule("lucid:authz-log",    authz_log_sql,    ["pgsql:authz-log-sink"]),
        rule("lucid:authz-denied", authz_denied_sql, ["pgsql:authz-denied-sink"]),

        # rejected messages — one rule per schema validation
        *[
            rule(desc, _rejection_sql(topic, schema), ["pgsql:rejected-messages-sink"])
            for desc, topic, schema in _REJECTION_RULES
        ],
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ensure_postgres_connector(client: EMQXClient) -> None:
    """Idempotently create the lucid-postgres connector in EMQX."""
    spec = {
        "type": "pgsql",
        "name": "lucid-postgres",
        "server": LUCID_DB_HOST,
        "database": LUCID_DB_NAME,
        "username": LUCID_DB_USER,
        "password": LUCID_DB_PASSWORD,
        "pool_size": 8,
        "enable": True,
    }
    r = client.get("/api/v5/connectors/pgsql:lucid-postgres")
    if r.status_code == 200:
        update_body = {k: v for k, v in spec.items() if k not in ("type", "name")}
        upd = client.put("/api/v5/connectors/pgsql:lucid-postgres", update_body)
        if upd.status_code not in (200, 201):
            print(f"  [ERROR] lucid-postgres connector update: {upd.text}", file=sys.stderr)
            sys.exit(1)
        print("  [update] lucid-postgres connector")
        return
    resp = client.post("/api/v5/connectors", spec)
    if resp.status_code not in (200, 201):
        print(f"  [ERROR] lucid-postgres connector: {resp.text}", file=sys.stderr)
        sys.exit(1)
    print("  [ok] lucid-postgres connector created")


# ---------------------------------------------------------------------------
# Smart Data Hub — Schema Registry
# ---------------------------------------------------------------------------

def _metric_cfg_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "enabled":                   {"type": "boolean"},
            "interval_s":                {"type": "number"},
            "change_threshold_percent":  {"type": "number"},
        },
        "additionalProperties": False,
    }


def schemas() -> list[dict]:
    def s(name: str, schema: dict) -> dict:
        return {"name": name, "type": "json", "source": json.dumps(schema)}

    return [
        s("lucid-command", {
            "type": "object",
            "required": ["request_id"],
            "properties": {
                "request_id": {"type": "string"},
            },
            "additionalProperties": True,
        }),
        s("lucid-agent-metadata", {
            "type": "object",
            "required": ["version", "platform", "architecture"],
            "properties": {
                "version":      {"type": "string"},
                "platform":     {"type": "string"},
                "architecture": {"type": "string"},
                "ip_address":   {"type": "string"},
            },
            "additionalProperties": True,
        }),
        s("lucid-agent-status", {
            "type": "object",
            "required": ["state"],
            "properties": {
                "state":               {"type": "string"},
                "connected_since_ts":  {"type": "string"},
                "uptime_s":            {"type": "number"},
            },
            "additionalProperties": True,
        }),
        s("lucid-agent-state", {
            "type": "object",
            "properties": {
                "components": {},
            },
            "additionalProperties": True,
        }),
        s("lucid-agent-cfg", {
            "type": "object",
            "required": ["heartbeat_s"],
            "properties": {
                "heartbeat_s": {"type": "number"},
            },
            "additionalProperties": True,
        }),
        s("lucid-agent-cfg-logging", {
            "type": "object",
            "required": ["log_level"],
            "properties": {
                "log_level": {"type": "string", "enum": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]},
            },
            "additionalProperties": False,
        }),
        s("lucid-agent-cfg-telemetry", {
            "type": "object",
            "properties": {
                "cpu_percent":    _metric_cfg_schema(),
                "memory_percent": _metric_cfg_schema(),
                "disk_percent":   _metric_cfg_schema(),
            },
            "additionalProperties": True,
        }),
        s("lucid-telemetry", {
            "type": "object",
            "required": ["value"],
            "properties": {
                "value": {},
            },
            "additionalProperties": True,
        }),
        s("lucid-event-result", {
            "type": "object",
            "required": ["request_id", "ok"],
            "properties": {
                "request_id": {"type": "string"},
                "ok":         {"type": "boolean"},
                "action":     {"type": "string"},
                "error":      {"type": ["string", "null"]},
                "applied":    {},
            },
            "additionalProperties": True,
        }),
        s("lucid-component-metadata", {
            "type": "object",
            "required": ["version"],
            "properties": {
                "version":      {"type": "string"},
                "capabilities": {},
            },
            "additionalProperties": True,
        }),
        s("lucid-component-status", {
            "type": "object",
            "required": ["state"],
            "properties": {
                "state": {"type": "string"},
            },
            "additionalProperties": True,
        }),
        s("lucid-schema-topic", {
            "type": "object",
            "properties": {
                # Agents emit publishes/subscribes as objects keyed by topic
                # name, not arrays. Keep the validator permissive on the
                # top-level shape; the per-topic structure is checked at
                # consumer-time.
                "publishes":  {"type": "object"},
                "subscribes": {"type": "object"},
            },
            "additionalProperties": True,
        }),
    ]


def ensure_schema(client: EMQXClient, spec: dict) -> None:
    name = spec["name"]
    resp = client.get(f"/api/v5/schema_registry/{name}")
    if resp.status_code == 200:
        update_body = {k: v for k, v in spec.items() if k != "name"}
        upd = client.put(f"/api/v5/schema_registry/{name}", update_body)
        if upd.status_code not in (200, 201):
            print(f"  [ERROR]  schema update {name}: {upd.text}", file=sys.stderr)
            sys.exit(1)
        print(f"  [update] schema {name}")
        return
    resp = client.post("/api/v5/schema_registry", spec)
    if resp.status_code not in (200, 201):
        print(f"  [ERROR]  schema {name}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    print(f"  [create] schema {name}")


# ---------------------------------------------------------------------------
# Smart Data Hub — Schema Validation
# ---------------------------------------------------------------------------

def schema_validations() -> list[dict]:
    def v(name: str, topics: list[str], schema: str) -> dict:
        return {
            "name": name,
            "topics": topics,
            "enable": True,
            "strategy": "all_pass",
            # "ignore" so invalid messages reach the rule engine where
            # NOT schema_check() rejection rules capture them to Postgres.
            # The JSON normalization transformation still uses "drop" because
            # non-JSON payloads cannot be decoded or meaningfully stored.
            "failure_action": "ignore",
            "log_failure": {"level": "warning"},
            "checks": [{"type": "json", "schema": schema}],
        }

    return [
        # commands — all must carry request_id
        v("validate-agent-cmd",     ["lucid/agents/+/cmd/#"],               "lucid-command"),
        v("validate-component-cmd", ["lucid/agents/+/components/+/cmd/#"],  "lucid-command"),
        # agent retained
        v("validate-agent-metadata",      ["lucid/agents/+/metadata"],          "lucid-agent-metadata"),
        v("validate-agent-status",        ["lucid/agents/+/status"],            "lucid-agent-status"),
        v("validate-agent-state",         ["lucid/agents/+/state"],             "lucid-agent-state"),
        v("validate-agent-cfg",           ["lucid/agents/+/cfg"],               "lucid-agent-cfg"),
        v("validate-agent-cfg-logging",   ["lucid/agents/+/cfg/logging"],       "lucid-agent-cfg-logging"),
        v("validate-agent-cfg-telemetry", ["lucid/agents/+/cfg/telemetry"],     "lucid-agent-cfg-telemetry"),
        # agent streaming
        v("validate-agent-telemetry", ["lucid/agents/+/telemetry/#"],           "lucid-telemetry"),
        v("validate-agent-events",    ["lucid/agents/+/evt/#"],                 "lucid-event-result"),
        # component retained
        v("validate-component-metadata",      ["lucid/agents/+/components/+/metadata"],      "lucid-component-metadata"),
        v("validate-component-status",        ["lucid/agents/+/components/+/status"],        "lucid-component-status"),
        v("validate-component-cfg-logging",   ["lucid/agents/+/components/+/cfg/logging"],   "lucid-agent-cfg-logging"),
        v("validate-component-cfg-telemetry", ["lucid/agents/+/components/+/cfg/telemetry"], "lucid-agent-cfg-telemetry"),
        # component streaming
        v("validate-component-telemetry", ["lucid/agents/+/components/+/telemetry/#"], "lucid-telemetry"),
        v("validate-component-events",    ["lucid/agents/+/components/+/evt/#"],       "lucid-event-result"),
    ]


def ensure_schema_validation(client: EMQXClient, spec: dict) -> None:
    name = spec["name"]
    resp = client.get(f"/api/v5/schema_validations/validation/{name}")
    if resp.status_code == 200:
        upd = client.put("/api/v5/schema_validations", spec)
        if upd.status_code not in (200, 201):
            print(f"  [ERROR]  validation update {name}: {upd.text}", file=sys.stderr)
            sys.exit(1)
        print(f"  [update] validation {name}")
        return
    resp = client.post("/api/v5/schema_validations", spec)
    if resp.status_code not in (200, 201):
        print(f"  [ERROR]  validation {name}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    print(f"  [create] validation {name}")


# ---------------------------------------------------------------------------
# Smart Data Hub — Message Transformation
# ---------------------------------------------------------------------------

def message_transformations() -> list[dict]:
    return [
        {
            "name": "lucid-json-normalize",
            "topics": ["lucid/#"],
            "enable": True,
            "failure_action": "drop",
            "log_failure": {"level": "warning"},
            "payload_decoder": {"type": "json"},
            "payload_encoder": {"type": "json"},
            "operations": [],
        },
    ]


def ensure_message_transformation(client: EMQXClient, spec: dict) -> None:
    name = spec["name"]
    resp = client.get(f"/api/v5/message_transformations/transformation/{name}")
    if resp.status_code == 200:
        upd = client.put("/api/v5/message_transformations", spec)
        if upd.status_code not in (200, 201):
            print(f"  [ERROR]  transformation update {name}: {upd.text}", file=sys.stderr)
            sys.exit(1)
        print(f"  [update] transformation {name}")
        return
    resp = client.post("/api/v5/message_transformations", spec)
    if resp.status_code not in (200, 201):
        print(f"  [ERROR]  transformation {name}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    print(f"  [create] transformation {name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"Connecting to EMQX at {EMQX_URL} ...")
    client = EMQXClient()
    print("Authenticated.\n")

    ensure_postgres_connector(client)

    print("── Schemas ─────────────────────────────────────")
    for spec in schemas():
        ensure_schema(client, spec)

    print("\n── Schema Validations ──────────────────────────")
    for spec in schema_validations():
        ensure_schema_validation(client, spec)

    print("\n── Message Transformations ─────────────────────")
    for spec in message_transformations():
        ensure_message_transformation(client, spec)

    print("\n── Connectors ──────────────────────────────────")
    for spec in connectors():
        ensure_connector(client, spec)

    print("\n── Actions ─────────────────────────────────────")
    for spec in actions():
        ensure_action(client, spec)

    print("\n── Rules ───────────────────────────────────────")
    for spec in rules():
        ensure_rule(client, spec)

    print("\nDone. All LUCID EMQX resources are in place.")


if __name__ == "__main__":
    main()
