import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from import_dashboards import initialization_action


class MetabaseSetupTests(unittest.TestCase):
    def test_new_installation_needs_setup(self):
        properties = {"has-user-setup": False, "setup-token": "demo-token"}
        self.assertEqual(initialization_action(properties, {}), "setup")

    def test_existing_installation_is_preserved(self):
        properties = {"has-user-setup": True, "setup-token": "demo-token"}
        self.assertEqual(initialization_action(properties, {}), "skip_existing")

    def test_remaining_token_does_not_repeat_setup(self):
        properties = {"has-user-setup": True, "setup-token": "demo-token"}
        state = {"owned": True, "complete": True}
        self.assertEqual(initialization_action(properties, state), "skip_complete")

    def test_partial_import_can_continue(self):
        properties = {"has-user-setup": True}
        self.assertEqual(initialization_action(properties, {"owned": True}), "resume")
