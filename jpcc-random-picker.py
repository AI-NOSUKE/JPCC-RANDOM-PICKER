#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JPCC-RANDOM-PICKER v1.4-best-abeja

【概要】
ABEJA-CC-JA(S3上の日本語Common Crawl由来JSONL)から、
「指定キーワードを含む日本語文章」を低メモリかつランダム性を高めて抽出し、CSV保存します。

【データソース】
- abeja-cc-ja: ABEJAがCommon Crawlを前処理して構築した日本語Webコーパス。
  2019〜2023年のCommon Crawlを対象にした、日本語専用の大規模公開コーパスです。

【必要パッケージ】
    pip install boto3 orjson

【使い方】
対話モード:
    python jpcc-random-picker.py
    -> キーワードと件数を聞かれるので入力するだけ。

引数モード:
    python jpcc-random-picker.py -k ももクロ ももいろクローバーZ -n 1000
    オプション:
        -o / --outfile              出力CSV (default: output.csv)
        --max-minutes               時間上限・分 (default: 15)
        --oversample-factor         候補を何倍まで集めてから乱数スコアで絞るか (default: 3.0)

【v1.4-best-abejaの修正点】
- データソースをABEJA-CC-JA専用に整理。FineWeb-2関連の分岐・依存を削除。
- multiprocessing の spawn 環境(macOS/Windows等)でも、CLIで更新したCONFIGがworkerへ渡るよう修正。
- oversample_factor を導入し、指定件数ぴったりで即停止せず、候補を少し多めに見て乱数スコアで絞れるようにした。
- 時間上限・走査完了・中断時も、指定件数を満たしていればCSVへ保存して終了する。
- ABEJA-CC-JAで実用性の高い url / text / char_len をCSVに出力する。

【設計判断】
- 本ツールは、日本語生活者全体やSNS発話全体の代表サンプルを作るものではありません。
  抽出対象は、Common Crawlに捕捉され、ABEJA-CC-JAの前処理を通った日本語Webテキストです。
- 全件走査はしません。命中全件からの厳密な一様抽出を保証するよりも、
  手軽さ・速度・ランダムな部分観測のバランスを優先します。
- 非圧縮JSONLは、ランダム開始後に末尾まで読まず、固定サイズのウィンドウだけ読みます。
  これにより、ファイル後半ほど拾われやすい偏りを減らします。
- gzipはS3 Rangeで実質ランダムアクセスできないため、非圧縮JSONLよりランダム性・効率に制約があります。
  ここでは「疑似ランダム行スキップ」として扱います。
- bytes事前フィルタはデフォルトON。ただし生キーワードだけでなく、
  NFKC形・半角カナ形・全角英数形の変種も含めたパターンで検索するため、
  正規化で初めて一致する表記の取り逃しを実用上ほぼ防ぎます。
  最終判定はJSON解析後のNFKC+casefold本文で行います。
- oversample_factor > 1 の場合、指定件数より多めのユニーク候補を確認し、
  決定的乱数スコアが小さいものを指定件数だけ残します。速度優先なら --oversample-factor 1 を指定してください。
- Web由来のため、広告文、SEO記事、企業ページ、商品説明、崩れた本文が混ざる可能性はあります。
  生活者コメント風の材料として使う場合は、出力後に目視または別処理でクリーニングしてください。
"""

import os
import sys
import csv
import gzip
import time
import hashlib
import unicodedata
import re
import random
import argparse
import itertools
import heapq
import queue
import threading
from typing import List, Dict, Any, Generator, Tuple, Optional

from multiprocessing import Pool, cpu_count, Manager

# boto3はABEJA-CC-JA(S3)の読み込みに必要。
try:
    import boto3
    from botocore.client import Config
    from botocore import UNSIGNED
except ImportError:
    boto3 = None
    Config = None
    UNSIGNED = None

try:
    import orjson as json_lib
except ImportError:
    import json as json_lib

# ===============================================================
# 設定
# 通常ユーザーはCLIまたは対話入力で keywords / limit を指定するだけで使えます。
# outfile と max_runtime_sec はCLIオプションで変更可能。
# それ以外は通常変更不要の高度設定です。
# ===============================================================
CONFIG = {
    # --- ユーザー入力で上書きされる項目 ---
    "keywords": [],                   # OR検索 (CLI/対話で設定)
    "limit": 0,                       # 最終出力件数 (CLI/対話で設定)
    "outfile": "output.csv",          # 出力ファイル
    "max_runtime_sec": 15 * 60,       # 時間上限。超えたら途中結果で打ち切り。

    # --- 以下は通常変更不要の高度設定 ---
    "min_len": 100,                   # 最小文字数
    "max_len": 2000,                  # 最大文字数
    "seed": 42,                       # ランダムシード
    "num_downloaders": 2,             # 同時ダウンロード数
    "processes": cpu_count(),         # CPU並列数
    "chunk_size": 2000,               # 1バッチ行数
    "oversample_factor": 3.0,          # 候補確認数 = limit * oversample_factor

    # --- データソース ---
    "bucket": "abeja-cc-ja",          # ABEJA-CC-JAのS3バケット名

    # ランダムウィンドウ抽出設定(非圧縮JSONL)
    "window_bytes": 4 * 1024 * 1024,   # 1ウィンドウの最大取得量
    "windows_per_file": 8,             # 1ファイル・1パスあたりのウィンドウ数
    "max_lines_per_window": 20000,     # 1ウィンドウで読む最大行数

    # gzipはランダムアクセスできないため、別扱い。
    "gz_windows_per_file": 1,
    "max_gz_skip": 500_000,

    # 「粘る」設定: 件数未達なら、ウィンドウ位置を変えて再走査する最大パス数。
    # パスごとにRNGシードへパス番号を混ぜるため、毎パス別の場所を読む。
    "max_passes": 3,

    # 時間上限到達時、件数未達なら「延長するか」をユーザーに確認する。
    # y で1回あたりこの分数だけ延長(何度でも可)。60秒無応答なら自動終了するため、
    # 放置していても勝手に何時間も走り続けることはない。
    "extension_minutes": 10,
    "extension_prompt_timeout_sec": 60,

    # bytes事前フィルタ。変種パターン込みでデフォルトON。
    "use_bytes_prefilter": True,

    # UI描画間隔。ANSIエスケープで画面更新します。
    "ui_refresh_sec": 1.0,
}

_TEXT_KEYS = [
    "content", "text", "body", "article", "title",
    "raw_text", "message", "desc", "description",
]

PAT_BYTES, KEYWORDS_NFKC, TEXT_KEYS, G_STATUS_QUEUE = None, tuple(), tuple(_TEXT_KEYS), None


def stable_int(value: str, *, bytes_len: int = 16) -> int:
    """Pythonのhash()に依存しない決定的な整数ハッシュ。"""
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:bytes_len], "big", signed=False)


# ===============================================================
# キーワード変種生成 (事前フィルタの取り逃し対策)
# ===============================================================
def _build_fullwidth_to_halfwidth_kana() -> Dict[str, str]:
    """全角カタカナ -> 半角カタカナ の対応表。

    半角カナ(U+FF61..U+FF9F)をNFKC正規化すると全角になることを利用し、
    その逆写像を機械的に構築する。濁点・半濁点付き(ガ -> ｶﾞ 等)も含む。
    """
    mapping: Dict[str, str] = {}
    # 単独文字 (濁点・半濁点マーク自体は除外)
    for cp in range(0xFF61, 0xFFA0):
        if cp in (0xFF9E, 0xFF9F):
            continue
        hw = chr(cp)
        fw = unicodedata.normalize("NFKC", hw)
        if len(fw) == 1:
            mapping.setdefault(fw, hw)
    # 濁点・半濁点合成 (ｶ+ﾞ -> ガ 等)
    for cp in range(0xFF66, 0xFFA0):
        for mark in ("\uFF9E", "\uFF9F"):
            hw = chr(cp) + mark
            fw = unicodedata.normalize("NFKC", hw)
            if len(fw) == 1:
                mapping.setdefault(fw, hw)
    return mapping


_FW2HW_KANA = _build_fullwidth_to_halfwidth_kana()


def _to_halfwidth_kana(s: str) -> str:
    return "".join(_FW2HW_KANA.get(c, c) for c in s)


def _to_fullwidth_ascii(s: str) -> str:
    """ASCII英数記号 -> 全角 (Z -> Ｚ 等)。"""
    return "".join(
        chr(ord(c) + 0xFEE0) if 0x21 <= ord(c) <= 0x7E else c
        for c in s
    )


def keyword_byte_variants(keywords: List[str]) -> List[bytes]:
    """事前フィルタ用に、各キーワードの表記変種をUTF-8バイト列で列挙する。

    対象: 原文 / NFKC形 / 半角カナ形 / 全角英数形 / 半角カナ+全角英数形、
    およびそれぞれの小文字形・大文字形(Z/z、Ｚ/ｚ など)。
    最終判定はNFKC+casefold本文で行うため、ここは「落とさないための網」であり
    厳密一致である必要はない。
    """
    variants = set()
    for kw in keywords:
        if not kw:
            continue
        nfkc = unicodedata.normalize("NFKC", kw)
        forms = {
            kw,
            nfkc,
            _to_halfwidth_kana(nfkc),
            _to_fullwidth_ascii(nfkc),
            _to_fullwidth_ascii(_to_halfwidth_kana(nfkc)),
        }
        case_forms = set()
        for f in forms:
            case_forms.update({f, f.lower(), f.upper(), f.casefold()})
        for f in case_forms:
            if f:
                variants.add(f.encode("utf-8"))
    return sorted(variants)


def initializer(status_q, config_snapshot: Dict[str, Any]):
    """worker初期化。

    macOS/Windowsのspawn環境では、親プロセスで更新したCONFIGが子プロセスに
    自動では反映されない。必ずsnapshotを受け取って更新する。
    """
    global CONFIG, PAT_BYTES, KEYWORDS_NFKC, G_STATUS_QUEUE
    CONFIG.update(config_snapshot)
    G_STATUS_QUEUE = status_q

    if CONFIG.get("use_bytes_prefilter", True):
        parts = [re.escape(v) for v in keyword_byte_variants(CONFIG["keywords"])]
        PAT_BYTES = re.compile(b"|".join(parts)) if parts else re.compile(b"(?!a)a")
    else:
        PAT_BYTES = None

    KEYWORDS_NFKC = tuple(
        unicodedata.normalize("NFKC", kw).casefold()
        for kw in CONFIG["keywords"]
        if kw
    )


# ===============================================================
# UI
# ===============================================================
class UIManager:
    def __init__(self, num_workers: int, status_queue, deadline: float):
        self.num_workers = num_workers
        self.status_queue = status_queue
        self.deadline = deadline
        self.lock = threading.Lock()
        self.worker_stats = {
            i: {"lines": 0, "hits": 0, "errors": 0}
            for i in range(num_workers)
        }
        self.logs = []
        self.total_hits = 0
        self.candidates_seen = 0
        self.target_candidates = 0
        self.files_processed = 0
        self.total_files = 0
        self.current_pass = 1
        self.pace_per_min = 0.0
        self.eta_sec: Optional[float] = None
        self._paused = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def pause(self):
        """延長確認プロンプトなどの間、画面の書き換えを止める。"""
        self._paused.set()
        time.sleep(0.3)  # 描画中のrenderが終わるのを待つ
        sys.stdout.write("\033[H\033[J")
        sys.stdout.flush()

    def resume(self):
        self._paused.clear()

    def set_deadline(self, deadline: float):
        with self.lock:
            self.deadline = deadline

    def set_pace(self, pace_per_min: float, eta_sec: Optional[float]):
        with self.lock:
            self.pace_per_min = pace_per_min
            self.eta_sec = eta_sec

    def get_held_count(self) -> int:
        with self.lock:
            return self.total_hits

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join()
        self._render(final=True)

    def log(self, msg):
        with self.lock:
            self.logs.append(f"[{time.strftime('%H:%M:%S')}] {msg}")
            self.logs = self.logs[-4:]

    def update_from_queue(self):
        while True:
            try:
                update = self.status_queue.get_nowait()
                wid = update["id"]
                with self.lock:
                    self.worker_stats[wid]["lines"] += update.get("lines", 0)
                    self.worker_stats[wid]["hits"] += update.get("hits", 0)
                    self.worker_stats[wid]["errors"] += update.get("errors", 0)
            except queue.Empty:
                break
            except Exception:
                break

    def set_sample_size(self, n: int):
        with self.lock:
            self.total_hits = n

    def set_candidate_count(self, n: int):
        with self.lock:
            self.candidates_seen = n

    def set_target_candidates(self, n: int):
        with self.lock:
            self.target_candidates = n

    def set_total_files(self, n):
        with self.lock:
            self.total_files = n

    def set_pass(self, n: int):
        with self.lock:
            self.current_pass = n

    def increment_files(self, n=1):
        with self.lock:
            self.files_processed += n

    def _loop(self):
        refresh = max(0.2, float(CONFIG.get("ui_refresh_sec", 1.0)))
        while not self._stop.wait(refresh):
            self.update_from_queue()
            if not self._paused.is_set():
                self._render()

    def _render(self, final=False):
        # os.system('clear')は毎秒サブプロセスを起動して重いので使わない。
        sys.stdout.write("\033[H\033[J")
        print("=== JPCC Random Picker v1.4-best-abeja Final Result ===" if final
              else "=== JPCC Random Picker v1.4-best-abeja ===")
        with self.lock:
            remain = max(0, int(self.deadline - time.time()))
            print(f"  SUPPLY PASS: {self.current_pass}/{CONFIG['max_passes']}"
                  f"  | FILE WINDOWS: {self.files_processed} / {self.total_files}"
                  f"  | 残り時間上限: {remain//60}m{remain%60:02d}s")
            for i in range(self.num_workers):
                s = self.worker_stats[i]
                print(f"  [WORKER {i:02d}] | Lines:{s['lines']:>8,} | Hits:{s['hits']:>6,} | Errors:{s['errors']:>3,}")
            print("\n--- Logs ---")
            for m in self.logs:
                print(" ", m)
            print("----------------------------------")
            print(f"🎯 サンプル保持: {self.total_hits:,} / {CONFIG['limit']:,} 件")
            if self.target_candidates > CONFIG["limit"]:
                print(f"🔀 候補確認: {self.candidates_seen:,} / {self.target_candidates:,} 件")
            if not final and self.pace_per_min > 0:
                if self.eta_sec is not None:
                    eta_min = self.eta_sec / 60
                    mark = "✅ 上限内に到達見込み" if time.time() + self.eta_sec <= self.deadline \
                        else "⚠️ このペースだと上限内に届きません"
                    print(f"📈 ペース: {self.pace_per_min:.1f}件/分 | 到達まで約{eta_min:.0f}分 | {mark}")
                else:
                    print(f"📈 ペース: {self.pace_per_min:.1f}件/分")
            if final:
                print("\n✅ 終了しました。")
        sys.stdout.flush()


# ===============================================================
# Downloader / Feeder
# ===============================================================
def safe_put(q: queue.Queue, item, stop_event: threading.Event) -> bool:
    """Queue詰まり時にstop_eventを見ながらputする。"""
    while not stop_event.is_set():
        try:
            q.put(item, timeout=1)
            return True
        except queue.Full:
            continue
    return False


# ===============================================================
# abeja-cc-ja JSONL / JSONL.GZ ソース
# ===============================================================
def iter_jsonl_complete_lines_from_range_bytes(
    data: bytes,
    *,
    started_mid_file: bool,
    ended_before_eof: bool,
    max_lines: int,
) -> Generator[bytes, None, None]:
    """Rangeで得たbytesから、壊れていないJSONL行だけを返す。

    Rangeの開始位置・終了位置は行境界とは限りません。
    - 開始位置がファイル先頭でなければ、先頭の半端行を捨てる。
    - RangeがEOF前で終わる場合、末尾の半端行を捨てる。
    """
    if not data:
        return

    if started_mid_file:
        first_nl = data.find(b"\n")
        if first_nl < 0:
            return
        data = data[first_nl + 1:]

    if ended_before_eof and not data.endswith(b"\n"):
        last_nl = data.rfind(b"\n")
        if last_nl < 0:
            return
        data = data[:last_nl]

    for i, raw in enumerate(data.splitlines()):
        if i >= max_lines:
            break
        if raw:
            yield raw


def iter_random_windows_for_jsonl(
    s3,
    key: str,
    size: int,
    rng: random.Random,
    stop_event: threading.Event,
) -> Generator[bytes, None, None]:
    """非圧縮JSONLを、複数のランダムRangeウィンドウとして読む。"""
    if not size or size <= 0:
        return

    window_bytes = max(1, int(CONFIG["window_bytes"]))
    windows_per_file = max(1, int(CONFIG["windows_per_file"]))
    max_lines = max(1, int(CONFIG["max_lines_per_window"]))

    for _ in range(windows_per_file):
        if stop_event.is_set():
            break

        start_pos = rng.randint(0, max(0, size - 1))
        end_pos = min(size - 1, start_pos + window_bytes - 1)
        range_header = f"bytes={start_pos}-{end_pos}"

        obj = s3.get_object(Bucket=CONFIG["bucket"], Key=key, Range=range_header)
        data = obj["Body"].read()

        yield from iter_jsonl_complete_lines_from_range_bytes(
            data,
            started_mid_file=(start_pos > 0),
            ended_before_eof=(end_pos < size - 1),
            max_lines=max_lines,
        )


def iter_random_windows_for_gzip(
    s3,
    key: str,
    rng: random.Random,
    stop_event: threading.Event,
) -> Generator[bytes, None, None]:
    """gzip JSONLを、ランダム行スキップ後に一定行だけ読む。

    gzipはS3 Rangeで任意の未圧縮位置へseekできないため、
    「先頭からskipして一定行読む」方式。max_gz_skipを超える後半領域は
    選ばれにくいが、厳密性より手軽さ優先の扱い。
    """
    gz_windows = max(1, int(CONFIG.get("gz_windows_per_file", 1)))
    max_lines = max(1, int(CONFIG["max_lines_per_window"]))
    max_gz_skip = max(0, int(CONFIG["max_gz_skip"]))

    for _ in range(gz_windows):
        if stop_event.is_set():
            break

        obj = s3.get_object(Bucket=CONFIG["bucket"], Key=key)
        with gzip.open(obj["Body"], "rb") as f:
            skip_n = rng.randint(0, max_gz_skip)
            for _ in range(skip_n):
                if stop_event.is_set():
                    break
                if not f.readline():
                    break

            for i, raw in enumerate(f):
                if stop_event.is_set():
                    break
                if raw:
                    yield raw.rstrip(b"\n")
                if i + 1 >= max_lines:
                    break


def feeder_thread(
    key_queue: queue.Queue,
    all_objects: List[Tuple[str, int]],
    ui: "UIManager",
    stop_event: threading.Event,
):
    """件数到達まで「粘る」ための供給スレッド。

    同じファイル群を最大max_passes回、パスごとに別シャッフル・別ウィンドウ位置で
    再供給する。key_queueのmaxsizeにより、消費に合わせて供給される。
    """
    max_passes = max(1, int(CONFIG["max_passes"]))

    for pass_no in range(1, max_passes + 1):
        if stop_event.is_set():
            break
        ui.set_pass(pass_no)
        if pass_no > 1:
            ui.log(f"件数未達のため供給PASS {pass_no} を開始(別ウィンドウ位置で再走査)")

        order = list(all_objects)
        random.Random(stable_int(f"{CONFIG['seed']}::pass{pass_no}")).shuffle(order)

        for key, size in order:
            if not safe_put(key_queue, (key, size, pass_no), stop_event):
                return

    # 全パス供給完了。downloader数ぶんの終了通知を入れる。
    for _ in range(CONFIG["num_downloaders"]):
        if not safe_put(key_queue, None, stop_event):
            return


def downloader_thread(
    key_queue: queue.Queue,
    line_queue: queue.Queue,
    ui: UIManager,
    stop_event: threading.Event,
):
    if boto3 is None:
        ui.log("boto3未インストールのためABEJA-CC-JAを利用できません")
        safe_put(line_queue, None, stop_event)
        return

    s3 = boto3.client(
        "s3",
        config=Config(signature_version=UNSIGNED, retries={"max_attempts": 5}),
    )

    while not stop_event.is_set():
        try:
            item = key_queue.get(timeout=1)
        except queue.Empty:
            continue

        if item is None:
            break

        key, size, pass_no = item
        ui.log(f"処理中(P{pass_no}): s3://{CONFIG['bucket']}/{key}")

        # スレッドの実行順に依存しないよう、(ファイル, パス)単位でRNGを固定する。
        # パス番号を混ぜることで、再走査時は別のウィンドウ位置を読む。
        rng = random.Random(stable_int(f"{CONFIG['seed']}::{key}::pass{pass_no}"))

        try:
            if key.endswith(".gz"):
                iterator = iter_random_windows_for_gzip(s3, key, rng, stop_event)
            else:
                iterator = iter_random_windows_for_jsonl(s3, key, size, rng, stop_event)

            for raw in iterator:
                if not safe_put(line_queue, raw, stop_event):
                    break

            ui.increment_files()

        except Exception as e:
            ui.log(f"Download err: {e.__class__.__name__}: {str(e)[:80]}")

    # 正常終了時は必ずsentinelを入れる。
    if not stop_event.is_set():
        safe_put(line_queue, None, stop_event)

# ===============================================================
# Worker
# ===============================================================
def extract_text(obj: Dict[str, Any]) -> str:
    for key in TEXT_KEYS:
        value = obj.get(key)
        if isinstance(value, str) and value:
            return unicodedata.normalize("NFKC", value)
    return ""


def content_has_keyword(text: str) -> bool:
    # NFKC正規化済みの本文をさらにcasefoldし、大文字小文字の違いを無視して判定する。
    text_cf = text.casefold()
    return any(kw in text_cf for kw in KEYWORDS_NFKC)


def worker_process(args: Tuple[int, Tuple[bytes, ...]]) -> List[Dict[str, Any]]:
    wid, lines_batch = args
    results = []
    hits = 0
    err = 0

    for raw in lines_batch:
        # 変種込みbytes事前フィルタ(高速化)。最終判定はNFKC本文で行う。
        if PAT_BYTES is not None and not PAT_BYTES.search(raw):
            continue

        try:
            obj = json_lib.loads(raw)
            text = extract_text(obj)

            if not text:
                continue
            if not content_has_keyword(text):
                continue
            if not (CONFIG["min_len"] <= len(text) <= CONFIG["max_len"]):
                continue

            rid = obj.get("id") or obj.get("url") or "?"
            results.append({
                "id": rid,
                "url": obj.get("url") or "",
                "text": text,
            })
            hits += 1

        except Exception:
            err += 1
            continue

    G_STATUS_QUEUE.put({"id": wid, "lines": len(lines_batch), "hits": hits, "errors": err})
    return results


def line_generator(
    line_q: queue.Queue,
    num_downloaders: int,
    stop_event: threading.Event,
) -> Generator[bytes, None, None]:
    active = num_downloaders

    while active > 0 and not stop_event.is_set():
        try:
            line = line_q.get(timeout=5)
            if line is None:
                active -= 1
                continue
            yield line
        except queue.Empty:
            continue


def chunked(it, size):
    it = iter(it)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            return
        yield chunk


# ===============================================================
# Candidate sampling
# ===============================================================
def candidate_score(seed: int, rid: str, text_hash: str) -> int:
    return stable_int(f"{seed}::{rid}::{text_hash}", bytes_len=16)


def push_candidate(
    heap: List[Tuple[int, int, Dict[str, Any]]],
    row: Dict[str, Any],
    score: int,
    limit: int,
    tie: int,
):
    """scoreが小さい候補をlimit件保持する。heap内は最大scoreが先頭。"""
    item = (-score, tie, row)

    if len(heap) < limit:
        heapq.heappush(heap, item)
        return True

    worst_score = -heap[0][0]
    if score < worst_score:
        heapq.heapreplace(heap, item)
        return True

    return False


def write_rows_atomic(outfile: str, rows: List[Dict[str, Any]]) -> None:
    """CSVを一時ファイルへ書いてから置き換え、既存出力の消失を避ける。"""
    abs_outfile = os.path.abspath(outfile)
    out_dir = os.path.dirname(abs_outfile)
    tmp_path = os.path.join(out_dir, f".{os.path.basename(outfile)}.tmp")

    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "url", "text", "char_len"])
        for row in rows:
            writer.writerow([
                row.get("id", ""),
                row.get("url", ""),
                row.get("text", ""),
                row.get("char_len", ""),
            ])

    os.replace(tmp_path, abs_outfile)


# ===============================================================
# 入力処理
# ===============================================================
def timed_input(prompt: str, timeout_sec: float) -> Optional[str]:
    """timeout_sec以内に入力が無ければNoneを返すinput。

    非対話環境(stdinがttyでない)では即Noneを返す。
    """
    if not sys.stdin.isatty():
        return None

    result: List[str] = []

    def _read():
        try:
            result.append(input(prompt))
        except (EOFError, KeyboardInterrupt):
            pass

    t = threading.Thread(target=_read, daemon=True)
    t.start()
    t.join(timeout_sec)
    return result[0] if result else None


def parse_user_input():
    def positive_int(value: str) -> int:
        try:
            n = int(value.replace(",", ""))
        except ValueError as exc:
            raise argparse.ArgumentTypeError("正の整数を指定してください") from exc
        if n <= 0:
            raise argparse.ArgumentTypeError("正の整数を指定してください")
        return n

    def positive_float(value: str) -> float:
        try:
            n = float(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("正の数値を指定してください") from exc
        if n <= 0:
            raise argparse.ArgumentTypeError("正の数値を指定してください")
        return n

    p = argparse.ArgumentParser(
        description="JPCCからキーワードを含む文章をランダム抽出してCSV保存します。"
    )
    p.add_argument("-k", "--keywords", nargs="+",
                   help="検索キーワード(複数指定でOR検索)")
    p.add_argument("-n", "--limit", type=positive_int, help="出力件数")
    p.add_argument("-o", "--outfile", default="output.csv", help="出力CSVファイル名")
    p.add_argument("--max-minutes", type=positive_float, default=15.0,
                   help="時間上限(分)。超えたら途中結果で打ち切り (default: 15)")
    p.add_argument("--oversample-factor", type=positive_float, default=3.0,
                   help="指定件数の何倍まで候補を確認してから乱数スコアで絞るか。速度優先なら1 (default: 3.0)")
    args = p.parse_args()

    keywords = args.keywords
    limit = args.limit

    # 引数が無ければ対話で聞く
    if not keywords:
        raw = input("検索キーワードを入力してください(複数はスペース区切り、OR検索): ").strip()
        keywords = [w for w in re.split(r"[\s,、]+", raw) if w]
    if not limit:
        while True:
            raw = input("何件抽出しますか?(例: 1000): ").strip()
            try:
                limit = int(raw.replace(",", ""))
                if limit > 0:
                    break
            except ValueError:
                pass
            print("正の整数を入力してください。")

    if not keywords:
        print("キーワードが指定されていません。終了します。")
        sys.exit(1)

    CONFIG["keywords"] = keywords
    CONFIG["limit"] = limit
    CONFIG["outfile"] = args.outfile
    CONFIG["max_runtime_sec"] = max(60.0, args.max_minutes * 60.0)
    CONFIG["oversample_factor"] = max(1.0, args.oversample_factor)


# ===============================================================
# Main
# ===============================================================
def run():
    parse_user_input()

    deadline_box = [time.time() + CONFIG["max_runtime_sec"]]

    manager = Manager()
    status_q = manager.Queue()
    ui = UIManager(CONFIG["processes"], status_q, deadline_box[0])
    ui.start()
    ui.log("STEP1: 初期化中...")

    def list_abeja_objects() -> List[Tuple[str, int]]:
        if boto3 is None:
            raise RuntimeError("boto3が未インストールです (pip install boto3)")
        s3_lister = boto3.client(
            "s3",
            config=Config(signature_version=UNSIGNED, retries={"max_attempts": 5}),
        )
        paginator = s3_lister.get_paginator("list_objects_v2")
        return [
            (obj["Key"], int(obj.get("Size") or 0))
            for page in paginator.paginate(Bucket=CONFIG["bucket"])
            for obj in page.get("Contents", [])
            if obj["Key"].endswith((".jsonl", ".jsonl.gz"))
        ]

    ui.log("STEP2: S3リスト取得中 (abeja-cc-ja)...")
    all_objects: List[Tuple[str, int]] = []
    last_list_error: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            all_objects = list_abeja_objects()
            last_list_error = None
            break
        except Exception as e:
            last_list_error = e
            if attempt >= 3:
                break
            ui.log(
                f"S3リスト取得に失敗。再試行します... "
                f"({attempt}/3: {e.__class__.__name__})"
            )
            time.sleep(attempt)

    if last_list_error is not None:
        ui.stop()
        print(f"データソースのファイル一覧を取得できませんでした: {last_list_error}")
        sys.exit(1)

    if not all_objects:
        ui.stop()
        print("データソースのファイル一覧を取得できませんでした。ネットワークと依存パッケージを確認してください。")
        sys.exit(1)

    target_candidates = max(CONFIG["limit"], int(round(CONFIG["limit"] * CONFIG["oversample_factor"])))
    ui.set_target_candidates(target_candidates)
    ui.set_total_files(len(all_objects) * CONFIG["max_passes"])

    stop_event = threading.Event()
    # feederが消費ペースに合わせて供給するよう、key_qは有限長にする。
    key_q: queue.Queue = queue.Queue(maxsize=64)
    line_q: queue.Queue = queue.Queue(maxsize=20000)

    feeder = threading.Thread(
        target=feeder_thread,
        args=(key_q, all_objects, ui, stop_event),
        daemon=True,
    )
    feeder.start()

    downloaders = [
        threading.Thread(
            target=downloader_thread,
            args=(key_q, line_q, ui, stop_event),
            daemon=True,
        )
        for _ in range(CONFIG["num_downloaders"])
    ]
    for d in downloaders:
        d.start()

    # 時間上限の見張り。imap待ちで主ループが止まっていても確実に打ち切る。
    # 上限到達時に件数未達なら、延長するかをユーザーに確認する(無応答なら自動終了)。
    def watchdog():
        while not stop_event.wait(1):
            if time.time() < deadline_box[0]:
                continue

            held = ui.get_held_count()
            if held >= CONFIG["limit"]:
                # oversample待ちでも時間上限に来たら、指定件数は満たしているので主ループ側で止める。
                ui.log("⏱ 時間上限に到達。指定件数は満たしているため保存して終了します。")
                stop_event.set()
                break

            ext_min = CONFIG["extension_minutes"]
            prompt_timeout = CONFIG["extension_prompt_timeout_sec"]

            ui.pause()
            print(f"⏱ 時間上限に到達しましたが、現在 {held:,} / {CONFIG['limit']:,} 件です。")
            ans = timed_input(
                f"   あと{ext_min}分延長しますか? [y/N] ({prompt_timeout}秒無応答で自動終了): ",
                prompt_timeout,
            )

            if ans is not None and ans.strip().lower() in ("y", "yes"):
                deadline_box[0] = time.time() + ext_min * 60
                ui.set_deadline(deadline_box[0])
                ui.resume()
                ui.log(f"⏩ {ext_min}分延長しました。続行します。")
            else:
                ui.resume()
                ui.log("⏱ 時間上限に到達。途中結果で打ち切ります。")
                stop_event.set()
                break

    threading.Thread(target=watchdog, daemon=True).start()

    ui.log(
        f"STEP3: パイプライン開始 "
        f"(Downloader:{CONFIG['num_downloaders']}, Worker:{CONFIG['processes']}, "
        f"上限:{int(CONFIG['max_runtime_sec'] // 60)}分, "
        f"oversample:{CONFIG['oversample_factor']:.1f}x)"
    )

    start = time.time()
    seen_hashes = set()
    sample_heap: List[Tuple[int, int, Dict[str, Any]]] = []
    unique_candidates_seen = 0
    reached = False
    rows: List[Dict[str, Any]] = []

    try:
        with Pool(
            processes=CONFIG["processes"],
            initializer=initializer,
            initargs=(status_q, dict(CONFIG)),
        ) as pool:
            wid_cycle = itertools.cycle(range(CONFIG["processes"]))
            arg_gen = (
                (next(wid_cycle), batch)
                for batch in chunked(
                    line_generator(line_q, CONFIG["num_downloaders"], stop_event),
                    CONFIG["chunk_size"],
                )
            )

            for batch in pool.imap_unordered(worker_process, arg_gen):
                if stop_event.is_set():
                    break

                # ペースと到達見込みを更新(開始30秒後から)。
                elapsed = time.time() - start
                if elapsed >= 30 and unique_candidates_seen > 0:
                    pace = unique_candidates_seen / (elapsed / 60.0)
                    remaining_n = target_candidates - unique_candidates_seen
                    eta = (remaining_n / pace) * 60.0 if pace > 0 and remaining_n > 0 else None
                    ui.set_pace(pace, eta)

                for res in batch:
                    safe_text = res["text"].replace("\n", " ").replace("\r", " ")
                    text_hash = hashlib.sha1(safe_text.encode("utf-8")).hexdigest()
                    if text_hash in seen_hashes:
                        continue

                    seen_hashes.add(text_hash)
                    unique_candidates_seen += 1
                    ui.set_candidate_count(unique_candidates_seen)

                    rid = str(res["id"]) if res["id"] != "?" else text_hash[:16]
                    row = {
                        "id": rid,
                        "url": res.get("url", ""),
                        "text": safe_text,
                        "char_len": len(safe_text),
                    }
                    score = candidate_score(CONFIG["seed"], rid, text_hash)
                    push_candidate(sample_heap, row, score, CONFIG["limit"], unique_candidates_seen)
                    ui.set_sample_size(len(sample_heap))

                    # 指定件数を保持したうえで、oversample分の候補確認まで済んだら停止。
                    if len(sample_heap) >= CONFIG["limit"] and unique_candidates_seen >= target_candidates:
                        ui.log(
                            f"🎉 候補 {unique_candidates_seen:,} 件を確認。"
                            f"乱数スコアで {CONFIG['limit']:,} 件に絞って停止します。"
                        )
                        reached = True
                        stop_event.set()
                        break

    except KeyboardInterrupt:
        ui.log("中断されました。途中結果を保存します。")

    finally:
        stop_event.set()
        feeder.join(timeout=10)
        for d in downloaders:
            d.join(timeout=10)

        rows = [
            item[2]
            for item in sorted(sample_heap, key=lambda x: -x[0])
        ]

        write_rows_atomic(CONFIG["outfile"], rows)

        ui.set_sample_size(len(rows))
        ui.set_candidate_count(unique_candidates_seen)
        ui.stop()

    if reached:
        status = "指定候補数に到達"
    elif len(rows) >= CONFIG["limit"]:
        status = "指定件数に到達(時間上限/走査完了/中断によりoversample途中で終了)"
    else:
        status = "件数未達(時間上限/データ走査完了/中断)"

    print(
        f"\n✅ Done [{status}]: {len(rows)} rows -> {CONFIG['outfile']} "
        f"(candidates={unique_candidates_seen:,}, time={time.time()-start:.1f}s)"
    )
    if len(rows) < CONFIG["limit"]:
        print("   ヒント: --max-minutes を増やすか、キーワードを見直してください。")
    elif CONFIG["oversample_factor"] > 1 and not reached:
        print("   メモ: 指定件数は満たしましたが、時間上限/走査完了/中断によりoversampleは途中終了しました。")


if __name__ == "__main__":
    run()
