import csv
import io
import importlib.util
import json
import queue
import re
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "jpcc_random_picker",
    ROOT / "jpcc-random-picker.py",
)
jpcc = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(jpcc)


class OutputInfoTests(unittest.TestCase):
    def setUp(self):
        self._config = dict(jpcc.CONFIG)
        self._matchers = jpcc.KEYWORD_MATCHERS
        self._keywords_nfkc = jpcc.KEYWORDS_NFKC
        self._pat_bytes = jpcc.PAT_BYTES
        self._status_queue = jpcc.G_STATUS_QUEUE

    def tearDown(self):
        jpcc.CONFIG.clear()
        jpcc.CONFIG.update(self._config)
        jpcc.KEYWORD_MATCHERS = self._matchers
        jpcc.KEYWORDS_NFKC = self._keywords_nfkc
        jpcc.PAT_BYTES = self._pat_bytes
        jpcc.G_STATUS_QUEUE = self._status_queue

    def test_json_escaped_japanese_passes_bytes_prefilter(self):
        status_q = queue.Queue()
        jpcc.initializer(status_q, {
            "keywords": ["日本"],
            "min_len": 0,
            "max_len": 100,
            "use_bytes_prefilter": True,
        })
        escaped = json.dumps({"id": "lower", "text": "日本の文章"}, ensure_ascii=True).encode("ascii")
        upper = re.sub(
            rb"\\u([0-9a-fA-F]{4})",
            lambda m: b"\\u" + m.group(1).upper(),
            escaped.replace(b'"lower"', b'"upper"'),
        )

        rows = jpcc.worker_process((0, (escaped, upper)))
        stats = status_q.get_nowait()

        self.assertEqual([row["id"] for row in rows], ["lower", "upper"])
        self.assertEqual(stats["lines"], 2)
        self.assertEqual(stats["prefilter_passed"], 2)
        self.assertEqual(stats["json_decoded"], 2)
        self.assertEqual(stats["keyword_hits"], 2)

    def test_output_destination_is_rejected_before_long_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            missing = Path(tmpdir) / "missing" / "result.csv"
            with self.assertRaisesRegex(ValueError, "出力先フォルダがありません"):
                jpcc.validate_output_destination(str(missing))

    def test_license_and_faq_share_negotiated_term_policy(self):
        license_text = (ROOT / "LICENSE").read_text(encoding="utf-8")
        faq_text = (ROOT / "docs" / "License_FAQ_JA.md").read_text(encoding="utf-8")

        for text in (license_text, faq_text):
            self.assertIn("年額または買い切りを一律には定めず", text)
            self.assertIn("契約期間", text)
            self.assertIn("利用主体", text)
            self.assertIn("利用範囲", text)

    def test_matched_keywords_use_nfkc_casefold_and_original_order(self):
        keywords = ["ChatGPT", "ももクロ", "ももいろクローバーZ"]
        jpcc.KEYWORD_MATCHERS = jpcc.build_keyword_matchers(keywords)
        jpcc.KEYWORDS_NFKC = tuple(norm for _, norm in jpcc.KEYWORD_MATCHERS)

        text = "chatgptと、ももｸﾛ、ももいろクローバーzについて"

        self.assertEqual(
            jpcc.find_matched_keywords(text),
            ["ChatGPT", "ももクロ", "ももいろクローバーZ"],
        )

    def test_csv_header_and_info_text_are_written_without_network(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = Path(tmpdir) / "momoclo.csv"
            infofile = Path(jpcc.info_path_for_csv(str(outfile)))

            rows = [{
                "id": "1",
                "url": "https://example.com",
                "text": "本文",
                "char_len": 2,
                "matched_keywords": "ももクロ|ももいろクローバーZ",
            }]
            jpcc.write_rows_atomic(str(outfile), rows)

            with outfile.open(newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                self.assertEqual(
                    next(reader),
                    ["id", "url", "text", "char_len", "matched_keywords"],
                )
                self.assertEqual(next(reader)[4], "ももクロ|ももいろクローバーZ")

            self.assertEqual(infofile.name, "momoclo_info.txt")

            jpcc.CONFIG.update({
                "keywords": ["ももクロ", "ももいろクローバーZ"],
                "limit": 10,
                "outfile": str(outfile),
                "max_runtime_sec": 900,
                "oversample_factor": 3.0,
                "seed": 42,
                "min_len": 100,
                "max_len": 2000,
                "max_passes": 3,
            })
            jpcc.write_run_info_atomic(
                str(outfile),
                str(infofile),
                status="指定候補数に到達",
                rows_written=1,
                candidates_seen=30,
                elapsed_sec=1.23,
                target_candidates=30,
                scan_stats={
                    "lines": 100,
                    "prefilter_passed": 12,
                    "json_decoded": 11,
                    "keyword_hits": 3,
                    "hits": 1,
                    "errors": 1,
                },
                created_at="2026-06-29T00:00:00+09:00",
            )

            info_text = infofile.read_text(encoding="utf-8")
            self.assertIn("script_version: v1.5.1", info_text)
            self.assertIn("- normalization: NFKC + casefold", info_text)
            self.assertIn("  - ももクロ", info_text)
            self.assertIn("- target_period: 2019〜2023年のCommon Crawl由来データ", info_text)
            self.assertIn("- rows_written: 1", info_text)
            self.assertIn("- lines_scanned: 100", info_text)
            self.assertIn("- prefilter_passed: 12", info_text)
            self.assertIn("- keyword_hits: 3", info_text)

    def test_ui_can_render_with_windows_cp932_output(self):
        jpcc.CONFIG.update({"limit": 10, "max_passes": 3})
        ui = jpcc.UIManager(1, queue.Queue(), time.time() + 60)
        ui.set_sample_size(1)
        ui.set_target_candidates(30)
        ui.set_candidate_count(3)
        ui.set_pace(2.0, 60.0)
        ui.log("[到達] 候補を確認しました。")

        raw = io.BytesIO()
        output = io.TextIOWrapper(raw, encoding="cp932")
        original_stdout = sys.stdout
        try:
            sys.stdout = output
            ui._render(final=True)
            output.flush()
        finally:
            sys.stdout = original_stdout

        rendered = raw.getvalue().decode("cp932")
        self.assertIn("[完了] 終了しました。", rendered)


if __name__ == "__main__":
    unittest.main()
