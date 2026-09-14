"""Check service state, not merely the HTTP response code."""
import json
import urllib.request

with urllib.request.urlopen("http://localhost:8080/api/v2/monitor/health", timeout=5) as response:
    health = json.load(response)
for component in ("metadatabase", "scheduler", "dag_processor"):
    if health.get(component, {}).get("status") != "healthy":
        raise SystemExit(f"Unhealthy: {component}")
