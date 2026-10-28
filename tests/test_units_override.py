# -*- coding: utf-8 -*-
"""UNITS 外置三态：机上覆盖 / 缺省回退内置 / 坏文件不炸。
运行：python tests/test_units_override.py（或 pytest）。"""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fleetdash import data, alerts


class UnitsOverrideTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "units.json")
        self._old = (os.environ.get("FLEETDASH_UNITS_FILE"),
                     data._UNITS_OVERRIDES, data._UNITS_MTIME,
                     alerts.SVC_TH)
        os.environ["FLEETDASH_UNITS_FILE"] = self.path
        self.reset()

    def tearDown(self):
        env, ov, mt, th = self._old
        if env is None:
            os.environ.pop("FLEETDASH_UNITS_FILE", None)
        else:
            os.environ["FLEETDASH_UNITS_FILE"] = env
        data._UNITS_OVERRIDES, data._UNITS_MTIME = ov, mt
        alerts.SVC_TH = th
        self.tmp.cleanup()

    @staticmethod
    def reset():
        data._UNITS_OVERRIDES = None
        data._UNITS_MTIME = -1.0

    def write(self, obj):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
        self.reset()

    def test_missing_file_falls_back_to_builtin(self):
        self.reset()
        self.assertEqual(data.units_for("4c47"), data.UNITS["4c47"])

    def test_override_wins_per_key(self):
        self.write({"_说明": "doc", "nodeX": ["my-api"], "53fa": ["only-guardian"]})
        self.assertEqual(data.units_for("nodeX"), ["my-api"])
        self.assertEqual(data.units_for("53fa"), ["only-guardian"])
        self.assertEqual(data.units_for("4c47"), data.UNITS["4c47"])

    def test_bad_json_falls_back(self):
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("{oops")
        self.reset()
        self.assertEqual(data.units_for("4c47"), data.UNITS["4c47"])

    def test_underscore_and_nonlist_ignored(self):
        self.write({"_c": "doc", "weird": "not-a-list", "ok": ["u1"]})
        self.assertEqual(data.units_for("ok"), ["u1"])
        self.assertEqual(data.units_for("weird"), data.UNITS.get("weird") or [])

    def test_unit_check_skips_outside_roster(self):
        self.write({"nodeX": ["my-api"]})
        fired = []
        orig = alerts.notify_unit_down
        alerts.notify_unit_down = lambda k, u, st: fired.append((k, u))
        alerts.UNIT_STATE.clear()
        alerts.SVC_TH = 1
        try:
            alerts.unit_check("nodeX", {"units": {"ghost": "inactive",
                                                  "my-api": "inactive"},
                                        "guardian_now": "NORMAL"})
        finally:
            alerts.notify_unit_down = orig
        self.assertEqual(fired, [("nodeX", "my-api")])


if __name__ == "__main__":
    unittest.main(verbosity=2)
