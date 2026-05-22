
import os
from fastapi.testclient import TestClient

# Set Chromium path for local dev
os.environ["BASENOTES_CHROMIUM_PATH"] = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
os.environ["FRAGRANTICA_CHROMIUM_HEADLESS"] = "0"

from api import app
import json

client = TestClient(app)

print("--- Running Fragrantica Diagnostics (WITH MINT) ---")
resp = client.get("/api/diagnostics/fragrantica?mint=1")
data = resp.json()
print(f"Mint Attempt Success: {data.get('mint_attempt', {}).get('success')}")
if not data.get('mint_attempt', {}).get('success'):
    print(f"Error: {data.get('mint_attempt', {}).get('error')}")

print("\n--- Testing search that should now use scraper ---")
# Reset scraper to ensure fresh attempt
import fragrance_parser_full_rewrite_fixed as engine
engine.reset_fragrantica_scraper(clear_cache=True)

resp = client.get("/api/fragrances/search?q=xerjoff")
print(f"Search Results: {len(resp.json().get('results', []))}")
