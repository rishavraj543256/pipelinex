import os
import unittest


@unittest.skipUnless(os.environ.get("PIPELINEX_TEST_API") == "1", "Requires migrated PostgreSQL")
class DemoAPI(unittest.TestCase):
    def test_isolation_quota_and_recovery(self):
        from fastapi.testclient import TestClient
        from pipelinex.api import app

        with TestClient(app) as client:
            def new_session():
                token = client.post("/demo/sessions").json()["token"]
                return {"Authorization": f"Bearer {token}"}

            self.assertEqual(client.get("/ready").status_code, 200)
            first, second = new_session(), new_session()
            response = client.post("/demo/runs", json={"failure": "gold"}, headers=first)
            self.assertEqual(response.status_code, 201)
            result = response.json()
            self.assertEqual(result["status"], "recovered")
            self.assertEqual(result["restored_stages"], ["bronze", "silver"])
            path = f"/demo/runs/{result['id']}"
            self.assertEqual(client.get(path, headers=first).json(), result)
            self.assertEqual(client.get(path, headers=second).status_code, 404)
            for _ in range(4):
                self.assertEqual(client.post("/demo/runs", json={"records": 1}, headers=first).status_code, 201)
            self.assertEqual(client.post("/demo/runs", json={}, headers=first).status_code, 429)
