#!/usr/bin/env python3
"""
Unit tests for the WoW "Forever" version filter in warcraft_wiki_sync.py.

Run:  python3 warcraft_wiki_sync_test.py
These tests are pure / network-free: they exercise classify_version(),
in_forever() and process_candidate() with synthetic category+wikitext inputs,
and version_gate_report() with monkeypatched fetch helpers.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import warcraft_wiki_sync as w


class ClassifyVersionTests(unittest.TestCase):
    def test_forever_patch_bucket(self):
        cls = w.classify_version(["Category:Content added by a Forever patch"])
        self.assertTrue(cls["forever"])
        self.assertEqual(cls["version"], "Forever")
        self.assertIn("Forever patch bucket", cls["signals"])

    def test_added_in_patch_16x(self):
        cls = w.classify_version(["Category:Added in patch 1.60.1"])
        self.assertTrue(cls["forever"])
        self.assertEqual(cls["version"], "Forever")
        self.assertIn("Added-in-patch-1.6x", cls["signals"])

    def test_classic_category_in_scope(self):
        # Category:Classic membership makes the page in-scope Classic (kept).
        cls = w.classify_version(["Category:Classic"])
        self.assertFalse(cls["forever"])
        self.assertTrue(cls["classic"])
        self.assertTrue(cls["keep"])
        self.assertEqual(cls["version"], "classic")

    def test_classic_items_category_in_scope(self):
        cls = w.classify_version(["Category:Classic items"])
        self.assertTrue(cls["classic"])
        self.assertTrue(cls["keep"])
        self.assertEqual(cls["version"], "classic")

    def test_classic_template_in_scope(self):
        # {{Classic only}} marks Classic presence in wikitext (secondary signal).
        cls = w.classify_version([], "{{Classic only|patch=4.0.3a>}}")
        self.assertTrue(cls["classic"])
        self.assertTrue(cls["keep"])
        self.assertEqual(cls["version"], "classic")

    def test_classic_unique_template_in_scope(self):
        cls = w.classify_version([], "{{Classic unique}}")
        self.assertTrue(cls["classic"])
        self.assertTrue(cls["keep"])

    def test_classic_season_template_in_scope(self):
        cls = w.classify_version([], "{{Classic season|name=Foo}}")
        self.assertTrue(cls["classic"])
        self.assertTrue(cls["keep"])

    def test_backported_item_in_forever_and_wotlk(self):
        # A WotLK-origin item backported to Forever carries BOTH the Forever
        # patch bucket (1.60.1) and the Classic-line ancestor bucket
        # ("Content added by a Classic patch") plus its specific leaf patch
        # (3.1.0). Still Forever; other_versions records the non-Forever line.
        cls = w.classify_version([
            "Category:Added in patch 1.60.1",
            "Category:Content added by a Classic patch",
            "Category:Added in patch 3.1.0",
        ])
        self.assertTrue(cls["forever"])
        self.assertEqual(cls["version"], "Forever")
        self.assertEqual(cls["other_versions"], "classic")

    def test_forever_precedence_over_classic(self):
        # A page carrying BOTH a Forever signal and Category:Classic resolves to
        # Forever (highest precedence) but is still flagged classic=True.
        cls = w.classify_version([
            "Category:Added in patch 1.60.1",
            "Category:Classic",
        ])
        self.assertTrue(cls["forever"])
        self.assertTrue(cls["classic"])
        self.assertTrue(cls["keep"])
        self.assertEqual(cls["version"], "Forever")

    def test_data_block_forever_marker(self):
        cls = w.classify_version([], wikitext="{{#data:#Forever |name=Foo}}")
        self.assertTrue(cls["forever"])
        self.assertIn("#data:#Forever", cls["signals"])

    def test_retail_rejected(self):
        cls = w.classify_version(["Category:Content added by patch",
                                  "Category:Items"])
        self.assertFalse(cls["forever"])
        self.assertEqual(cls["version"], "retail")
        self.assertFalse(cls["keep"])

    def test_classic_line_in_scope(self):
        # Content added by a Classic patch = Vanilla-era Classic content, now
        # in scope (kept as Classic). No longer rejected outright.
        cls = w.classify_version(["Category:Content added by a Classic patch",
                                  "Category:Added in patch 2.5.1"])
        self.assertFalse(cls["forever"])
        self.assertTrue(cls["classic"])
        self.assertTrue(cls["keep"])
        self.assertEqual(cls["version"], "classic")

    def test_multiple_non_forever_buckets(self):
        cls = w.classify_version(["Category:Content added by patch",
                                  "Category:Content added by a Classic patch"])
        self.assertFalse(cls["forever"])
        self.assertEqual(cls["version"], "multi")
        self.assertFalse(cls["keep"])

    def test_no_version_signal(self):
        cls = w.classify_version(["Category:Mount items"])
        self.assertFalse(cls["forever"])
        self.assertEqual(cls["version"], "unknown")

    def test_empty(self):
        self.assertFalse(w.classify_version([])["forever"])
        self.assertFalse(w.classify_version([], "")["forever"])


class InForeverTests(unittest.TestCase):
    def test_true_and_false(self):
        self.assertTrue(w.in_forever(["Category:Added in patch 1.60.1"]))
        self.assertTrue(w.in_forever([], "{{#data:#Forever|name=x}}"))
        self.assertFalse(w.in_forever(["Category:Content added by patch"]))
        self.assertFalse(w.in_forever(["Category:Content added by a Classic patch"]))


class ProcessCandidateTests(unittest.TestCase):
    def test_strict_keep_forever(self):
        d = w.process_candidate("P", "path", "c",
                                ["Category:Added in patch 1.60.1"], fetch_ok=True)
        self.assertTrue(d["keep"])
        self.assertTrue(d["version_verified"])
        self.assertEqual(d["reason"], "forever")

    def test_strict_keep_classic(self):
        # Classic pages are in scope (kept); reason reflects version "classic".
        d = w.process_candidate("P", "Classic/C", "c",
                                ["Category:Classic", "Category:Classic items"],
                                fetch_ok=True)
        self.assertTrue(d["keep"])
        self.assertTrue(d["version_verified"])
        self.assertEqual(d["reason"], "classic")

    def test_strict_reject_non_scope(self):
        d = w.process_candidate("P", "path", "c",
                                ["Category:Content added by patch"], fetch_ok=True)
        self.assertFalse(d["keep"])
        self.assertTrue(d["version_verified"])
        self.assertTrue(d["reason"].startswith("not-forever:"))

    def test_bucket_path_fallback_keeps_but_unverified(self):
        # categories could not be fetched -> keep via bucket path
        d = w.process_candidate("P", "path", "c", None, fetch_ok=False)
        self.assertTrue(d["keep"])
        self.assertFalse(d["version_verified"])
        self.assertTrue(d["fallback"])

    def test_fallback_with_data_block_verified(self):
        d = w.process_candidate("P", "path", "{{#data:#Forever|x}}",
                                None, fetch_ok=False)
        self.assertTrue(d["keep"])
        self.assertTrue(d["version_verified"])


class SafeTitleTests(unittest.TestCase):
    def test_slugify(self):
        self.assertEqual(w.safe_title("Black Skeletal Horse"), "Black_Skeletal_Horse")
        self.assertEqual(w.safe_title("A Difficult Path (Forever)"), "A_Difficult_Path_Forever")
        self.assertEqual(w.safe_title("Copper Bar"), "Copper_Bar")
        self.assertEqual(w.safe_title("__weird//title__"), "weird_title")


class VersionGateReportTests(unittest.TestCase):
    """version_gate_report() with monkeypatched fetch so it is network-free."""

    def _make_sync(self, rows):
        """rows: list of (title, path, content, categories, fetch_ok)."""
        state = {"rows": rows, "i": 0}

        def fake_enumerate():
            return [(r[0], r[1]) for r in state["rows"]]

        def fake_fetch(title):
            for r in state["rows"]:
                if r[0] == title:
                    return r[2], "2026-10-20T01:00:00Z", r[3], r[4]
            return None, None, None, False

        w.enumerate_forever_pages = fake_enumerate
        w.get_page_with_categories = fake_fetch
        return fake_enumerate, fake_fetch

    def test_report_splits_kept_and_rejected(self):
        self._make_sync([
            ("Forever Item", "Forever bucket/1.60.1", "c",
             ["Category:Added in patch 1.60.1"], True),
            ("Retail Item", "Forever bucket/1.60.1", "c",
             ["Category:Content added by patch"], True),   # wrong bucket -> reject
            ("Backported", "Forever bucket/1.60.1", "c",
             ["Category:Added in patch 1.60.1", "Category:Added in patch 3.1.0"], True),
            ("Vanilla NoMarker", "Forever bucket/1.60.1", "c",
             ["Category:Classic crafted items"], True),     # no marker -> reject
        ])
        rep = w.version_gate_report([(r[0], r[1]) for r in
                                     [("Forever Item", "p"), ("Retail Item", "p"),
                                      ("Backported", "p"), ("Vanilla NoMarker", "p")]])
        self.assertEqual(len(rep["kept"]), 2)
        self.assertEqual(len(rep["rejected"]), 2)
        rejected_reasons = {r[0] for r in rep["rejected"]}
        self.assertEqual(rejected_reasons, {"Retail Item", "Vanilla NoMarker"})
        kept_titles = {r[0] for r in rep["kept"]}
        self.assertEqual(kept_titles, {"Forever Item", "Backported"})

    def test_report_passthrough_when_all_forever(self):
        rows = [("A", "p", "c", ["Category:Added in patch 1.60.1"], True),
                ("B", "p", "c", ["Category:Content added by a Forever patch"], True)]
        self._make_sync(rows)
        rep = w.version_gate_report([("A", "p"), ("B", "p")])
        self.assertEqual(len(rep["kept"]), 2)
        self.assertEqual(len(rep["rejected"]), 0)
        self.assertEqual(rep["unverified"], [])


class ScopeExpansionDiscoveryTests(unittest.TestCase):
    """Proves the merged scope discovers BOTH Forever and Classic pages, merged
    without duplication. Network-free via monkeypatched API helpers."""

    FAKE_CMS = {
        "Category:Content added by a Forever patch": [
            {"ns": 0, "title": "Forever Item A"},
            {"ns": 14, "title": "Category:Forever sub"},
        ],
        "Category:Forever sub": [
            {"ns": 0, "title": "Forever Item B"},
        ],
        "Category:Classic": [
            {"ns": 0, "title": "Classic Item A"},
            {"ns": 14, "title": "Category:Classic items"},
            {"ns": 14, "title": "Category:Classic NPCs"},
        ],
        "Category:Classic items": [
            {"ns": 0, "title": "Classic Item C"},
        ],
        "Category:Classic NPCs": [
            {"ns": 0, "title": "Classic NPC D"},
        ],
    }
    CATS = {
        "Forever Item A": ["Category:Added in patch 1.60.1"],
        "Forever Item B": ["Category:Content added by a Forever patch"],
        "Classic Item A": ["Category:Classic", "Category:Classic items"],
        "Classic Item C": ["Category:Classic items"],
        "Classic NPC D": ["Category:Classic", "Category:Classic NPCs"],
    }

    def setUp(self):
        self._orig_api = w.wiki_api_get
        self._orig_get = w.get_page_with_categories
        # Fresh copy so this test's side effects (e.g. test_shared_page_...) don't
        # leak into other tests sharing the class-level FAKE_CMS.
        self.FAKE_CMS = {k: list(v) for k, v in self.__class__.FAKE_CMS.items()}

    def tearDown(self):
        w.wiki_api_get = self._orig_api
        w.get_page_with_categories = self._orig_get

    def _install(self):
        def fake_wiki_api_get(params):
            return {"query": {"categorymembers":
                              self.FAKE_CMS.get(params.get("cmtitle"), [])}}
        def fake_get(title):
            return ("content", "2026-10-20T01:00:00Z",
                    self.CATS.get(title, []), True)
        w.wiki_api_get = fake_wiki_api_get
        w.get_page_with_categories = fake_get

    def test_enumerate_discovers_both_forever_and_classic(self):
        self._install()
        merged, forever, classic = w.enumerate_scope_pages()
        merged_titles = {p for p, _ in merged}
        for t in ("Forever Item A", "Forever Item B",
                  "Classic Item A", "Classic Item C", "Classic NPC D"):
            self.assertIn(t, merged_titles)
        self.assertEqual({p for p, _ in forever},
                         {"Forever Item A", "Forever Item B"})
        self.assertEqual({p for p, _ in classic},
                         {"Classic Item A", "Classic Item C", "Classic NPC D"})

    def test_merged_without_duplicate_titles(self):
        # "Classic items" is reachable both as a subcat of Category:Classic and as
        # a direct root; a page under it must appear exactly once after dedup.
        self._install()
        merged, _, _ = w.enumerate_scope_pages()
        titles = [p for p, _ in merged]
        self.assertEqual(titles.count("Classic Item C"), 1)
        self.assertEqual(len(titles), len(set(titles)))

    def test_version_gate_reports_both_forever_and_classic(self):
        self._install()
        merged, _, _ = w.enumerate_scope_pages()
        report = w.version_gate_report(merged)
        self.assertEqual({r[0] for r in report["kept"]},
                         {"Forever Item A", "Forever Item B"})
        self.assertEqual({r[0] for r in report["classic"]},
                         {"Classic Item A", "Classic Item C", "Classic NPC D"})
        self.assertEqual(report["rejected"], [])

    def test_shared_page_counts_as_forever_not_classic(self):
        # A page reached via both the Forever and Classic roots appears once,
        # under the Forever path (title-dedup keeps the first-seen path).
        self._install()
        self.FAKE_CMS["Category:Forever sub"].append({"ns": 0, "title": "Shared Item"})
        merged, forever, classic = w.enumerate_scope_pages()
        titles = [p for p, _ in merged]
        self.assertEqual(titles.count("Shared Item"), 1)     # deduped to one
        self.assertIn("Shared Item", {p for p, _ in forever})
        self.assertNotIn("Shared Item", {p for p, _ in classic})


class BaseEntityOverrideDedupTests(unittest.TestCase):
    """Classic base / Forever override inheritance (task t_2746df22).

    A page and its flavor-twin (e.g. 'X' vs 'X (Forever)') resolve to one base
    entity; when both are in scope the Forever variant wins and the Classic base
    is superseded. A standalone Classic page with no Forever twin is kept.
    """

    def _dec(self, page, cats, wikitext=""):
        cls = w.classify_version(cats, wikitext)
        return {"page": page, "path": "Bucket/x", "content": "", "ts": "",
                "categories": cats, "fetch_ok": True, "last_ts": None,
                "decision": {"keep": cls["keep"], "classify": cls},
                "classify": cls}

    def test_normalize_base_strips_flavor_suffix(self):
        self.assertEqual(w.normalize_base("Druid abilities (Forever)"), "Druid abilities")
        self.assertEqual(w.normalize_base("Druid abilities (Classic)"), "Druid abilities")
        self.assertEqual(w.normalize_base("Thalyssra (Classic items)"), "Thalyssra")
        self.assertEqual(w.normalize_base("Druid abilities"), "Druid abilities")
        self.assertEqual(w.normalize_base("Gravestone Scepter"), "Gravestone Scepter")

    def test_normalize_base_preserves_non_flavor_parens(self):
        # Non-forever/classic parentheticals must NOT strip (avoid merging titles).
        self.assertEqual(w.normalize_base("Aragorn (son of Arathorn)"),
                         "Aragorn (son of Arathorn)")

    def test_forever_supersedes_classic_twin(self):
        # 'Druid abilities (Forever)' overrides its 'Druid abilities (Classic)' base.
        decs = [
            self._dec("Druid abilities (Classic)", ["Category:Classic"]),
            self._dec("Druid abilities (Forever)", ["Category:Added in patch 1.60.1"]),
        ]
        survivors, superseded = w.group_and_dedupe(decs)
        self.assertEqual([s["page"] for s in survivors], ["Druid abilities (Forever)"])
        self.assertEqual(len(superseded), 1)
        self.assertEqual(superseded[0][0], "Druid abilities (Classic)")
        self.assertIn("superseded", superseded[0][2])

    def test_standalone_classic_kept_when_no_forever(self):
        # Gravestone Scepter: standalone Classic, no Forever twin -> kept as base.
        decs = [self._dec("Gravestone Scepter", ["Category:Classic"])]
        survivors, superseded = w.group_and_dedupe(decs)
        self.assertEqual([s["page"] for s in survivors], ["Gravestone Scepter"])
        self.assertEqual(superseded, [])

    def test_classic_first_then_forever_order_independent(self):
        # Precedence must hold regardless of discovery order (Classic seen first).
        decs = [
            self._dec("Legacy Item (Forever)", ["Category:Added in patch 1.60.1"]),
            self._dec("Legacy Item (Classic)", ["Category:Content added by a Classic patch"]),
        ]
        survivors, superseded = w.group_and_dedupe(decs)
        self.assertEqual([s["page"] for s in survivors], ["Legacy Item (Forever)"])
        self.assertEqual([s[0] for s in superseded], ["Legacy Item (Classic)"])

    def test_two_forever_twins_dedupe_to_one(self):
        # Same base entity, both Forever -> keep one, supersede the other.
        decs = [
            self._dec("X (Forever)", ["Category:Added in patch 1.60.1"]),
            self._dec("X", ["Category:Content added by a Forever patch"]),
        ]
        survivors, superseded = w.group_and_dedupe(decs)
        self.assertEqual(len(survivors), 1)
        self.assertEqual(len(superseded), 1)

    def test_distinct_entities_not_merged(self):
        decs = [
            self._dec("Aragorn (Forever)", ["Category:Added in patch 1.60.1"]),
            self._dec("Gandalf (Forever)", ["Category:Added in patch 1.60.1"]),
        ]
        survivors, _ = w.group_and_dedupe(decs)
        self.assertEqual({s["page"] for s in survivors},
                         {"Aragorn (Forever)", "Gandalf (Forever)"})

    def test_out_of_scope_entity_dropped(self):
        # A base entity whose only variants are retail -> dropped entirely.
        decs = [self._dec("Retail Thing (Forever-ish)", ["Category:Content added by patch"])]
        survivors, superseded = w.group_and_dedupe(decs)
        self.assertEqual(survivors, [])
        self.assertEqual(len(superseded), 1)
        self.assertEqual(superseded[0][2], "out-of-scope")

    def test_scope_label_override_and_base(self):
        forever_cls = w.classify_version(["Category:Added in patch 1.60.1", "Category:Classic"])
        classic_cls = w.classify_version(["Category:Classic"])
        retail_cls = w.classify_version(["Category:Content added by patch"])
        self.assertEqual(w.scope_label(forever_cls), "Forever override")
        self.assertEqual(w.scope_label(classic_cls), "Classic base")
        self.assertEqual(w.scope_label(retail_cls), "retail")


if __name__ == "__main__":
    unittest.main(verbosity=2)