#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JPCC-RANDOM-PICKER

【概要】
S3上の巨大JSONL(.jsonl / .jsonl.gz)から「指定キーワードを含む文章」を
低メモリかつランダム性を担保して抽出、CSV保存します。

【ランダム性】
- ファイル順は毎回シャッフル
- 非圧縮(.jsonl): S3 Rangeでランダム開始（真のseek）
- 圧縮(.jsonl.gz): ストリーミング解凍しつつ、ランダム行スキップして開始（擬似seek）

【特徴】
- メモリ効率：巨大ファイルでもストリーミング処理
- ランダム性：ファイル間 + ファイル内で偏りを軽減
- 実務向け：到達件数で早期終了、毎回違うサンプルが得られる
"""

import os, csv, gzip, time, hashlib, unicodedata, re, random, itertools
from typing import List, Dict, Any, Generator, Tuple
from multiprocessing import Pool, Queue, cpu_count, Manager
from queue import Empty
import threading

import boto3
from botocore.client import Config
from botocore import UNSIGNED
try:
    import orjson as json_lib
except ImportError:
    import json as json_lib

# ===============================================================
# ユーザー設定
# ===============================================================
CONFIG = {
    "keywords": ["ももクロ", "ももいろクローバーZ"],  # OR検索
    "limit": 10000,                   # 収集目標件数
    "outfile": "output.csv",          # 出力ファイル
    "min_len": 100,                   # 最小文字数
    "max_len": 2000,                  # 最大文字数
    "seed": 42,                       # ランダムシード
    "num_downloaders": 2,             # 同時ダウンロード数
    "processes": cpu_count(),         # CPU並列数
    "chunk_size": 2000,               # 1バッチ行数
    "bucket": "abeja-cc-ja",          # バケット名
    "max_gz_skip": 500_000,           # gzip時の最大スキップ行数
}

_TEXT_KEYS = ["content","text","body","article","title",
              "raw_text","message","desc","description"]

PAT_BYTES, TEXT_KEYS, G_STATUS_QUEUE = None, tuple(_TEXT_KEYS), None

def initializer(status_q: Queue):
    global PAT_BYTES, G_STATUS_QUEUE
    G_STATUS_QUEUE = status_q
    parts = [re.escape(kw.encode()) for kw in CONFIG["keywords"] if kw]
    PAT_BYTES = re.compile(b"|".join(parts)) if parts else re.compile(b"(?!a)a")

# ===============================================================
# UI
# ===============================================================
class UIManager:
    def __init__(self, num_workers: int, status_queue: Queue):
        self.num_workers = num_workers; self.status_queue = status_queue
        self.lock = threading.Lock()
        self.worker_stats = {i: {"lines":0,"hits":0,"errors":0} for i in range(num_workers)}
        self.logs, self.total_hits, self.files_processed, self.total_files = [],0,0,0
        self._stop = threading.Event(); self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self): self._thread.start()
    def stop(self): self._stop.set(); self._thread.join(); self._render(final=True)
    def log(self,msg): self.logs.append(f"[{time.strftime('%H:%M:%S')}] {msg}"); self.logs=self.logs[-3:]
    def update_from_queue(self):
        while True:
            try:
                update=self.status_queue.get_nowait(); wid=update['id']
                with self.lock:
                    self.worker_stats[wid]['lines']+=update.get('lines',0)
                    self.worker_stats[wid]['hits']+=update.get('hits',0)
                    self.worker_stats[wid]['errors']+=update.get('errors',0)
            except Empty: break
            except Exception: break
    def increment_total_hits(self,n=1): 
        with self.lock: self.total_hits+=n
    def set_total_files(self,n): self.total_files=n
    def increment_files(self,n=1): 
        with self.lock: self.files_processed+=n
    def _loop(self):
        while not self._stop.wait(1): self.update_from_queue(); self._render()
    def _render(self,final=False):
        os.system('cls' if os.name=='nt' else 'clear')
        print("=== JPCC Random Picker Final Result ===" if final else "=== JPCC Random Picker (Pipeline Mode) ===")
        with self.lock:
            print(f"  FILE PROGRESS: {self.files_processed} / {self.total_files}")
            for i in range(self.num_workers):
                s=self.worker_stats[i]
                print(f"  [WORKER {i:02d}] | Lines:{s['lines']:>8,} | Hits:{s['hits']:>6,} | Errors:{s['errors']:>3,}")
            print("\n--- Logs ---"); [print(" ",m) for m in self.logs]
            print("----------------------------------")
            print(f"🎯 目標: {self.total_hits:,} / {CONFIG['limit']:,} 件")
            if final: print("\n✅ 全て完了しました。")
        import sys; sys.stdout.flush()

# ===============================================================
# Downloader (改良P1)
# ===============================================================
def downloader_thread(key_queue: Queue, line_queue: Queue, ui: UIManager):
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED, retries={"max_attempts":5}))
    rng = random.Random(CONFIG["seed"])
    while True:
        try:
            key = key_queue.get()
            if key is None: break
            
            # 現在処理中のファイルをログに表示
            ui.log(f"処理中: s3://{CONFIG['bucket']}/{key}")

            head = s3.head_object(Bucket=CONFIG["bucket"], Key=key)
            size = head.get("ContentLength", None)

            # 非圧縮 → Rangeでseekランダム
            if not key.endswith(".gz") and size:
                start_pos = rng.randint(0, size-1)
                rng_hdr = f"bytes={start_pos}-{size-1}"
                obj = s3.get_object(Bucket=CONFIG["bucket"], Key=key, Range=rng_hdr)
                reader = (line.decode("utf-8", "ignore") for line in obj["Body"].iter_lines())
                next(reader, None)  # 半端行捨て
                for line in reader: line_queue.put(line.encode("utf-8"))

            # gzip → ランダム行スキップ
            else:
                obj = s3.get_object(Bucket=CONFIG["bucket"], Key=key)
                with gzip.open(obj["Body"], "rt", encoding="utf-8", errors="ignore") as f:
                    skip_n = rng.randint(0, CONFIG["max_gz_skip"])
                    for _ in range(skip_n):
                        if not f.readline(): break
                    for line in f: line_queue.put(line.encode("utf-8"))

            ui.increment_files()
        except Exception as e:
            ui.log(f"Download err: {e.__class__.__name__}")
    line_queue.put(None)

# ===============================================================
# Worker
# ===============================================================
def worker_process(args: Tuple[int, List[bytes]]) -> List[Dict[str, Any]]:
    wid, lines_batch = args; results=[]; hits=err=0
    for raw in lines_batch:
        if not PAT_BYTES.search(raw): continue
        try:
            obj=json_lib.loads(raw)
            text=unicodedata.normalize("NFKC", next((obj[k] for k in TEXT_KEYS if isinstance(obj.get(k),str)),""))
            if CONFIG["min_len"]<=len(text)<=CONFIG["max_len"]:
                results.append({"id":obj.get("id","?"),"text":text}); hits+=1
        except Exception: err+=1; continue
    G_STATUS_QUEUE.put({'id':wid,'lines':len(lines_batch),'hits':hits,'errors':err})
    return results

def line_generator(line_q:Queue,num:int)->Generator[bytes,None,None]:
    active=num
    while active>0:
        try:
            line=line_q.get(timeout=30)
            if line is None: active-=1; continue
            yield line
        except Empty:
            print("[WARN] 30秒無応答。ネットワーク確認を。"); break

def chunked(it,size):
    it=iter(it)
    while True:
        chunk=tuple(itertools.islice(it,size))
        if not chunk: return
        yield chunk

# ===============================================================
# Main
# ===============================================================
def run():
    manager=Manager(); status_q=manager.Queue(); ui=UIManager(CONFIG["processes"],status_q)
    ui.start(); ui.log("STEP1: 初期化中...")

    if os.path.exists(CONFIG["outfile"]): os.remove(CONFIG["outfile"])
    with open(CONFIG["outfile"],"w",newline="",encoding="utf-8") as f: csv.writer(f).writerow(["id","text","char_len"])

    ui.log("STEP2: S3リスト取得中...")
    s3_lister=boto3.client("s3",config=Config(signature_version=UNSIGNED))
    paginator=s3_lister.get_paginator("list_objects_v2")
    all_keys=[obj['Key'] for page in paginator.paginate(Bucket=CONFIG["bucket"])
              for obj in page.get("Contents",[]) if obj['Key'].endswith((".jsonl",".jsonl.gz"))]
    random.Random(CONFIG["seed"]).shuffle(all_keys); ui.set_total_files(len(all_keys))

    key_q, line_q = Queue(), Queue(maxsize=20000)
    [key_q.put(k) for k in all_keys]; [key_q.put(None) for _ in range(CONFIG["num_downloaders"])]
    downloaders=[threading.Thread(target=downloader_thread,args=(key_q,line_q,ui)) for _ in range(CONFIG["num_downloaders"])]
    [d.start() for d in downloaders]

    ui.log(f"STEP3: パイプライン開始 (Downloader:{CONFIG['num_downloaders']}, Worker:{CONFIG['processes']})")
    start=time.time(); seen=set()

    with Pool(processes=CONFIG["processes"],initializer=initializer,initargs=(status_q,)) as pool, \
         open(CONFIG["outfile"],"a",newline="",encoding="utf-8") as f:
        writer=csv.writer(f); wid_cycle=itertools.cycle(range(CONFIG["processes"]))
        arg_gen=((next(wid_cycle),batch) for batch in chunked(line_generator(line_q,CONFIG["num_downloaders"]),CONFIG["chunk_size"]))

        for batch in pool.imap_unordered(worker_process,arg_gen):
            if ui.total_hits>=CONFIG["limit"]: break
            for res in batch:
                safe=res['text'].replace("\n"," "); sha1=hashlib.sha1(safe.encode("utf-8")).hexdigest()
                if sha1 in seen: continue
                seen.add(sha1); rid=res['id'] if res['id']!="?" else sha1[:16]
                writer.writerow([rid,safe,len(safe)]); ui.increment_total_hits()
                if ui.total_hits>=CONFIG["limit"]: break

    [d.join(timeout=5) for d in downloaders]; ui.stop()
    print(f"\n✅ Done: {ui.total_hits} rows -> {CONFIG['outfile']} (time={time.time()-start:.1f}s)")

if __name__=="__main__": run()