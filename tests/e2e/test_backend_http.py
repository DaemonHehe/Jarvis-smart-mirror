"""HTTP surface regression tests for the isolated backend server."""

import json
import os
import sys
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))

from test_utils import TestServer


class TestBackendHttp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = TestServer(port=8994)
        cls.server.start(fast_mode=True)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_dashboard_is_served_from_repository_assets(self):
        with urllib.request.urlopen(f"{self.server.http_url}/dashboard") as response:
            body = response.read().decode("utf-8")

        self.assertEqual(response.status, 200)
        self.assertIn("Jarvis", body)
        self.assertIn("/dashboard/static/app.js", body)

    def test_standalone_mirror_is_the_root_product_ui(self):
        for path in ("/", "/mirror"):
            with urllib.request.urlopen(f"{self.server.http_url}{path}") as response:
                body = response.read().decode("utf-8")
            self.assertEqual(response.status, 200)
            self.assertIn("Jarvis Smart Mirror", body)
            self.assertIn("/mirror/static/app.js", body)
            self.assertNotIn("MagicMirror", body)

    def test_phone_face_is_served_without_camera_access(self):
        with urllib.request.urlopen(f"{self.server.http_url}/face") as response:
            body = response.read().decode("utf-8")

        self.assertEqual(response.status, 200)
        self.assertIn('id="face"', body)
        self.assertNotIn("getUserMedia", body)

    def test_pipeline_history_limit_is_bounded(self):
        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(f"{self.server.http_url}/api/pipeline/history?limit=101")

        self.assertEqual(context.exception.code, 422)
        payload = json.loads(context.exception.read().decode("utf-8"))
        self.assertIn("detail", payload)


if __name__ == "__main__":
    unittest.main()
