import csv
import importlib.util
import tempfile
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

    def tearDown(self):
        jpcc.CONFIG.clear()
        jpcc.CONFIG.update(self._config)
        jpcc.KEYWORD_MATCHERS = self._matchers
        jpcc.KEYWORDS_NFKC = self._keywords_nfkc

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
                created_at="2026-06-29T00:00:00+09:00",
            )

            info_text = infofile.read_text(encoding="utf-8")
            self.assertIn("script_version: v1.5-output-info", info_text)
            self.assertIn("- normalization: NFKC + casefold", info_text)
            self.assertIn("  - ももクロ", info_text)
            self.assertIn("- target_period: 2019〜2023年のCommon Crawl由来データ", info_text)
            self.assertIn("- rows_written: 1", info_text)


if __name__ == "__main__":
    unittest.main()
