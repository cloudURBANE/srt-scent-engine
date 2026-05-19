
from fastapi.testclient import TestClient
from api import app

client = TestClient(app)

print("--- Testing source: URL ID ---")
resp = client.post("/api/fragrances/details", json={
    "id": "source:https://www.fragrantica.com/perfume/Creed/Silver-Mountain-Water-1517.html"
})
print(f"Status: {resp.status_code}")
print(f"Body: {resp.text}")

print("\n--- Testing source_url fallback ---")
resp = client.post("/api/fragrances/details", json={
    "source_url": "https://www.fragrantica.com/perfume/Creed/Silver-Mountain-Water-1517.html"
})
print(f"Status: {resp.status_code}")
print(f"Body: {resp.json().get('name')}")
