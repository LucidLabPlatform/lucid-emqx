#!/usr/bin/env python3
"""
Verify that EMQX contains the resources defined by setup_rules.py.

This script distinguishes between:
1. Core LUCID resources provisioned into EMQX
2. MQTT authentication / authorization wiring

The minimal stack is expected to pass the core checks while reporting that
MQTT auth/authz is not configured.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path


SETUP_RULES = Path(__file__).with_name("setup_rules.py")


def _string_value(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _extract_named_calls(function_name: str, callee_name: str) -> list[str]:
    module = ast.parse(SETUP_RULES.read_text())
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            names: list[str] = []
            for child in ast.walk(node):
                if isinstance(child, ast.Call) and isinstance(child.func, ast.Name) and child.func.id == callee_name:
                    if child.args:
                        name = _string_value(child.args[0])
                        if name:
                            names.append(name)
            return names
    raise RuntimeError(f"Could not find function {function_name} in {SETUP_RULES}")


def _extract_transformation_names() -> list[str]:
    module = ast.parse(SETUP_RULES.read_text())
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == "message_transformations":
            names: list[str] = []
            for child in ast.walk(node):
                if isinstance(child, ast.Dict):
                    for key, value in zip(child.keys, child.values):
                        if _string_value(key) == "name":
                            name = _string_value(value)
                            if name:
                                names.append(name)
            return names
    raise RuntimeError(f"Could not find function message_transformations in {SETUP_RULES}")


@dataclass(frozen=True)
class ExpectedResources:
    connector_id: str
    schemas: list[str]
    validations: list[str]
    transformations: list[str]
    actions: list[str]
    rule_descriptions: list[str]


def _extract_rejection_rule_names() -> list[str]:
    """Extract rule descriptions from the module-level _REJECTION_RULES list."""
    module = ast.parse(SETUP_RULES.read_text())
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_REJECTION_RULES":
                    names: list[str] = []
                    if isinstance(node.value, ast.List):
                        for elt in node.value.elts:
                            if isinstance(elt, ast.Tuple) and elt.elts:
                                name = _string_value(elt.elts[0])
                                if name:
                                    names.append(name)
                    return names
    return []


def expected_resources() -> ExpectedResources:
    return ExpectedResources(
        connector_id="pgsql:lucid-postgres",
        schemas=_extract_named_calls("schemas", "s"),
        validations=_extract_named_calls("schema_validations", "v"),
        transformations=_extract_transformation_names(),
        actions=_extract_named_calls("actions", "_pgsql_action"),
        rule_descriptions=_extract_named_calls("rules", "rule") + _extract_rejection_rule_names(),
    )


class EmqxAPI:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = self._login(username, password)

    def _request(self, method: str, path: str, body: dict | None = None) -> object:
        url = f"{self.base_url}{path}"
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.token}",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(req) as resp:
                payload = resp.read()
                if not payload:
                    return None
                return json.loads(payload.decode())
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode() if exc.fp else ""
            raise RuntimeError(f"{method} {path} failed with HTTP {exc.code}: {detail}") from exc

    def _login(self, username: str, password: str) -> str:
        url = f"{self.base_url}/api/v5/login"
        req = urllib.request.Request(
            url,
            data=json.dumps({"username": username, "password": password}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as resp:
                payload = json.loads(resp.read().decode())
                return payload["token"]
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode() if exc.fp else ""
            raise RuntimeError(f"Login failed with HTTP {exc.code}: {detail}") from exc

    def get(self, path: str) -> object:
        return self._request("GET", path)


def _check_named_resources(api: EmqxAPI, title: str, names: list[str], path_template: str) -> list[str]:
    missing: list[str] = []
    print(f"{title}:")
    for name in names:
        path = path_template.format(name=urllib.parse.quote(name, safe=""))
        try:
            api.get(path)
            print(f"  OK   {name}")
        except RuntimeError as exc:
            missing.append(name)
            print(f"  MISS {name} -> {exc}")
    return missing


def _check_rules(api: EmqxAPI, expected_descs: list[str]) -> list[str]:
    print("Rules:")
    payload = api.get("/api/v5/rules?limit=1000")
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected rules payload: {payload!r}")
    data = payload.get("data", [])
    actual = {item.get("description") for item in data if isinstance(item, dict)}
    missing: list[str] = []
    for desc in expected_descs:
        if desc in actual:
            print(f"  OK   {desc}")
        else:
            missing.append(desc)
            print(f"  MISS {desc}")
    return missing


def _check_connector(api: EmqxAPI, connector_id: str) -> tuple[bool, str]:
    print("Connector:")
    try:
        payload = api.get(f"/api/v5/connectors/{connector_id}")
    except RuntimeError as exc:
        print(f"  MISS {connector_id} -> {exc}")
        return False, "missing"
    status = payload.get("status", "?") if isinstance(payload, dict) else "?"
    print(f"  OK   {connector_id} status={status}")
    return True, str(status)


def _check_authn(api: EmqxAPI) -> tuple[bool, bool, bool, list[str]]:
    payload = api.get("/api/v5/authentication")
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected authentication payload: {payload!r}")
    ids: list[str] = []
    has_built_in = False
    has_ldap = False
    for item in payload:
        if not isinstance(item, dict):
            continue
        mechanism = item.get("mechanism", "?")
        backend = item.get("backend", "?")
        ids.append(f"{mechanism}:{backend}")
        if backend == "built_in_database":
            has_built_in = True
        if backend == "ldap":
            has_ldap = True
    return bool(payload), has_built_in, has_ldap, ids


def _check_authz(api: EmqxAPI) -> tuple[bool, str | None, list[str]]:
    settings = api.get("/api/v5/authorization/settings")
    sources = api.get("/api/v5/authorization/sources")
    if not isinstance(settings, dict):
        raise RuntimeError(f"Unexpected authorization settings payload: {settings!r}")
    if not isinstance(sources, dict):
        raise RuntimeError(f"Unexpected authorization sources payload: {sources!r}")
    source_items = sources.get("sources", [])
    source_types: list[str] = []
    has_built_in = False
    for item in source_items:
        if not isinstance(item, dict):
            continue
        source_type = str(item.get("type", "?"))
        source_types.append(source_type)
        if source_type == "built_in_database" and item.get("enable", True):
            has_built_in = True
    return has_built_in, settings.get("no_match"), source_types


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify LUCID EMQX resources and MQTT auth/authz state.")
    parser.add_argument("--url", default="http://localhost:18083", help="EMQX base URL, default: http://localhost:18083")
    parser.add_argument("--username", required=True, help="EMQX dashboard/API username")
    parser.add_argument("--password", required=True, help="EMQX dashboard/API password")
    parser.add_argument(
        "--profile",
        choices=("minimal", "full"),
        default="minimal",
        help="`minimal` checks core provisioning only; `full` requires broker-native auth and built-in authorization",
    )
    args = parser.parse_args()

    expected = expected_resources()
    api = EmqxAPI(args.url, args.username, args.password)

    failed = False

    connector_ok, connector_status = _check_connector(api, expected.connector_id)
    if not connector_ok:
        failed = True

    missing_schemas = _check_named_resources(api, "Schemas", expected.schemas, "/api/v5/schema_registry/{name}")
    missing_validations = _check_named_resources(
        api, "Schema Validations", expected.validations, "/api/v5/schema_validations/validation/{name}"
    )
    missing_transformations = _check_named_resources(
        api,
        "Message Transformations",
        expected.transformations,
        "/api/v5/message_transformations/transformation/{name}",
    )
    missing_actions = _check_named_resources(api, "Actions", expected.actions, "/api/v5/actions/pgsql:{name}")
    missing_rules = _check_rules(api, expected.rule_descriptions)

    if missing_schemas or missing_validations or missing_transformations or missing_actions or missing_rules:
        failed = True

    print("MQTT Authentication:")
    authn_any, authn_built_in, authn_ldap, authn_ids = _check_authn(api)
    if authn_ids:
        for authn_id in authn_ids:
            print(f"  FOUND {authn_id}")
    else:
        print("  NONE")

    print("MQTT Authorization:")
    authz_built_in, no_match, source_types = _check_authz(api)
    print(f"  no_match={no_match}")
    if source_types:
        for source_type in source_types:
            print(f"  FOUND {source_type}")
    else:
        print("  NONE")

    if args.profile == "full":
        if not authn_built_in:
            print("FAIL: full profile expects a built-in-database MQTT authenticator.")
            failed = True
        if not authz_built_in:
            print("FAIL: full profile expects a built-in-database MQTT authorization source.")
            failed = True
        if no_match != "deny":
            print(f"FAIL: full profile expects authorization no_match=deny, got {no_match!r}.")
            failed = True
        if authn_ldap:
            print("INFO: LDAP MQTT authenticator detected.")
        else:
            print("INFO: LDAP MQTT authenticator not configured.")
    else:
        if not authn_any:
            print("INFO: minimal profile does not require MQTT authenticators.")
        if not source_types:
            print("INFO: minimal profile does not require authorization sources.")

    if connector_status not in {"connected", "running", "?"}:
        print(f"WARN: connector status is {connector_status!r}.")

    print("")
    if failed:
        print("Verification failed.")
        return 1

    print("Verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
