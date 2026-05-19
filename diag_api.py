
from fastapi.testclient import TestClient
from api import app
import json

client = TestClient(app)

print("--- Running Upstream Diagnostics ---")
resp = client.get("/api/diagnostics/upstream")
print(json.dumps(resp.json(), indent=2))

print("\n--- Running Fragrantica Diagnostics (no mint) ---")
resp = client.get("/api/diagnostics/fragrantica")
print(json.dumps(resp.json(), indent=2))

print("\n--- Running Basenotes Diagnostics ---")
resp = client.get("/api/diagnostics/basenotes")
print(json.dumps(resp.json(), indent=2))
