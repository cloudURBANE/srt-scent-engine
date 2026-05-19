
import os
from fastapi.testclient import TestClient

os.environ["BASENOTES_CHROMIUM_PATH"] = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
os.environ["FRAGRANTICA_CHROMIUM_HEADLESS"] = "1"

from api import app
import json

client = TestClient(app)

print("--- Running Fragrantica Diagnostics (WITH MINT) ---")
resp = client.get("/api/diagnostics/fragrantica?mint=1")
data = resp.json()
print(json.dumps(data, indent=2))
