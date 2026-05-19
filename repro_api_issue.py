
from fastapi.testclient import TestClient
import json
import base64
from api import app

client = TestClient(app)

def test_query(q):
    print(f"\n--- Testing Query: {q} ---")
    response = client.get(f"/api/fragrances/search?q={q}")
    if response.status_code != 200:
        print(f"FAILED: status {response.status_code}")
        print(response.text)
        return
    
    data = response.json()
    results = data.get("results", [])
    print(f"Found {len(results)} results")
    
    if results:
        pick = results[0]
        print(f"Picking first result: {pick['name']} by {pick['house']} ({pick['id'][:20]}...)")
        
        # Verify id decoding
        try:
            decoded = json.loads(base64.urlsafe_b64decode(pick['id'].encode('ascii')).decode('utf-8'))
            print(f"Decoded ID: {decoded}")
        except Exception as e:
            print(f"ID Decoding FAILED: {e}")

        # Request details
        print("Requesting details...")
        detail_resp = client.post("/api/fragrances/details", json={
            "id": pick["id"],
            "source_url": pick["source_url"]
        })
        if detail_resp.status_code != 200:
            print(f"Detail Request FAILED: status {detail_resp.status_code}")
            print(detail_resp.text)
        else:
            detail_data = detail_resp.json()
            print(f"Detail Request SUCCESS: {detail_data.get('name')} - {detail_data.get('house')}")
            # print(json.dumps(detail_data, indent=2)[:500] + "...")
            coverage = detail_data.get("source_coverage", {})
            print(f"Source Coverage: {coverage}")

test_query("xerjoff")
test_query("santal 33")
