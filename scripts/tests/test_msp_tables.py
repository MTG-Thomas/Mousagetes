#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for the generated MSP wire-table adoption (muse-code-msp).

Stdlib unittest only. Pins that the allowlists are derived from the
bundle rather than hand-maintained, plus the daemon-start schema gate
and the typed wire-error classification. Never starts a daemon.
"""

from __future__ import annotations

import importlib.util
import shutil
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "muse-msp.py"


def load_module():
    spec = importlib.util.spec_from_file_location("muse_msp", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


muse_msp = load_module()

msp_wire = load_module()._MSP_WIRE


class BundleProvenanceTest(unittest.TestCase):
    def test_tables_come_from_the_bundle(self) -> None:
        self.assertEqual(set(muse_msp.MSP_METHODS), set(msp_wire.METHODS))
        self.assertIs(muse_msp.MSP_NOTIFICATIONS, msp_wire.NOTIFICATIONS)
        self.assertIs(muse_msp.MSP_ERRORS, msp_wire.ERRORS)
        self.assertEqual(
            muse_msp.EXPERIMENTAL_SCHEMA_FINGERPRINT, msp_wire.SCHEMA_FINGERPRINT
        )

    def test_command_set_derived_from_generated_params(self) -> None:
        for method in muse_msp.COMMAND_METHODS:
            params_name = msp_wire.METHODS[method]["params"]
            params_cls = getattr(msp_wire, params_name)
            self.assertIn("commandId", params_cls.__required_keys__, method)
        for method in set(muse_msp.MSP_METHODS) - set(muse_msp.COMMAND_METHODS):
            params_name = msp_wire.METHODS[method]["params"]
            if not params_name:
                continue
            params_cls = getattr(msp_wire, params_name)
            self.assertNotIn("commandId", params_cls.__required_keys__, method)

    def test_login_start_no_longer_mints_command_id(self) -> None:
        # The schema requires only {type} for account/loginStart; the old
        # hand-rolled table over-minted a commandId the server never asked
        # for. The derived table follows the bundle.
        self.assertNotIn("account/loginStart", muse_msp.COMMAND_METHODS)
        muse_msp.validate_call("account/loginStart", {"type": "deviceCode"}, "off")

    def test_command_spot_checks(self) -> None:
        self.assertIn("turn/start", muse_msp.COMMAND_METHODS)
        self.assertIn("goal/pause", muse_msp.COMMAND_METHODS)
        self.assertNotIn("usage/read", muse_msp.COMMAND_METHODS)
        self.assertNotIn("initialize", muse_msp.COMMAND_METHODS)
        self.assertNotIn("session/list", muse_msp.COMMAND_METHODS)


class WireErrorTest(unittest.TestCase):
    def test_known_code_classifies(self) -> None:
        err = muse_msp.classify_wire_error(
            {"code": -32021, "message": "busy", "data": {"kind": "sessionInUse"}}
        )
        self.assertIsInstance(err, RuntimeError)
        self.assertEqual(err.kind, "sessionInUse")
        self.assertEqual(err.code, -32021)
        self.assertFalse(err.retryable)

    def test_retryable_flag_from_table(self) -> None:
        err = muse_msp.classify_wire_error({"code": -32031, "message": "slow"})
        self.assertEqual(err.kind, "backpressured")
        self.assertTrue(err.retryable)

    def test_frame_kind_wins_over_code(self) -> None:
        err = muse_msp.classify_wire_error(
            {"code": -32603, "message": "x", "data": {"kind": "pageEventTooLarge"}}
        )
        self.assertEqual(err.kind, "pageEventTooLarge")

    def test_unknown_code_falls_back(self) -> None:
        err = muse_msp.classify_wire_error({"code": -99999, "message": "weird"})
        self.assertEqual(err.kind, "internal")
        self.assertFalse(err.retryable)


def _surfaces(fingerprint="fp", methods=(), notifications=()):
    return {
        "stable": {
            "fingerprint": fingerprint,
            "schemaVersion": 1,
            "methods": set(methods),
            "notifications": set(notifications),
        },
        "experimental": {
            "fingerprint": fingerprint,
            "schemaVersion": 1,
            "methods": set(methods),
            "notifications": set(notifications),
        },
    }


class SchemaCompatTest(unittest.TestCase):
    def _exact(self):
        return {
            "stable": {
                "fingerprint": muse_msp.STABLE_SCHEMA_FINGERPRINT,
                "schemaVersion": 1,
                "methods": set(muse_msp._STABLE_METHODS),
                "notifications": set(muse_msp._STABLE_NOTIFICATIONS),
            },
            "experimental": {
                "fingerprint": muse_msp.EXPERIMENTAL_SCHEMA_FINGERPRINT,
                "schemaVersion": 1,
                "methods": set(muse_msp.MSP_METHODS),
                "notifications": set(muse_msp.MSP_NOTIFICATIONS),
            },
        }

    def test_exact_match_is_clean(self) -> None:
        self.assertEqual(muse_msp.check_schema_compat(self._exact()), [])

    def test_missing_method_is_reported_not_fatal(self) -> None:
        # Cookbook fingerprint-mismatch posture: drift warns, never bricks.
        surfaces = self._exact()
        surfaces["experimental"]["methods"] = set(muse_msp.MSP_METHODS) - {"turn/start"}
        drift = muse_msp.check_schema_compat(surfaces)
        self.assertEqual(len(drift), 1)
        self.assertEqual(drift[0]["missingMethods"], ["turn/start"])

    def test_unrunnable_export_raises_typed(self) -> None:
        with self.assertRaises(muse_msp.SchemaDriftError) as ctx:
            muse_msp.export_host_schema(["/nonexistent/muse-bin"])
        self.assertEqual(ctx.exception.kind, "schemaDrift")

    def test_additive_drift_reports_without_raising(self) -> None:
        surfaces = self._exact()
        surfaces["experimental"]["notifications"] = set(muse_msp.MSP_NOTIFICATIONS) | {
            "session/listChanged"
        }
        surfaces["experimental"]["fingerprint"] = "sha256:changed"
        drift = muse_msp.check_schema_compat(surfaces)
        self.assertEqual(len(drift), 1)
        self.assertEqual(drift[0]["surface"], "experimental")
        self.assertEqual(drift[0]["extraNotifications"], ["session/listChanged"])
        self.assertIn("fingerprint", drift[0])

    @unittest.skipUnless(shutil.which("muse"), "muse binary not on PATH")
    def test_live_export_matches_bundle_methods(self) -> None:
        surfaces = muse_msp.export_host_schema(["muse"])
        self.assertEqual(surfaces["experimental"]["methods"], set(muse_msp.MSP_METHODS))
        # The drift the gate reports today, if any, must be additive-only:
        # check_schema_compat raises on anything breaking.
        muse_msp.check_schema_compat(surfaces)


if __name__ == "__main__":
    unittest.main()
