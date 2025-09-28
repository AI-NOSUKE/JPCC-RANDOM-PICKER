# JPCC-RANDOM-PICKER
<!-- Badges -->
[![CI](https://github.com/AI-NOSUKE/JPCC-RANDOM-PICKER/actions/workflows/ci.yml/badge.svg)](https://github.com/AI-NOSUKE/JPCC-RANDOM-PICKER/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/AI-NOSUKE/JPCC-RANDOM-PICKER)](https://github.com/AI-NOSUKE/JPCC-RANDOM-PICKER/releases)
[![License: JPCC-Limited-License](https://img.shields.io/badge/License-JPCC--Limited--License-green)](docs/License_FAQ_JA.md)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)


巨大データセットから指定のキーワードを含むテキストを「ランダム」に高速抽出する、次世代型ピッカーです。  
Japanese Common Crawl (JPCC) データセット（2019–2023年）から、キーワードを含む文章を偏りなくランダム抽出できます。

👉JPCC-RANDOM-PICKERで抽出したテキストを、PVMで分類した活用例レポート：ももクロ関連コメントの分類を例に実施
https://github.com/AI-NOSUKE/PVM/blob/main/docs/momoclo_report.md ,(Guitub内のPVMのページにリンクしています) 

---

## ✨ 特徴

- **真のランダム性を実現**  
  - ファイル順をシャッフル  
  - ファイル内でもランダム開始位置を選択  
  → データセット全体から偏りのないサンプルを取得可能

- **超高速パイプライン処理**  
  ダウンロード・検索・保存を並列化。複数のダウンローダーとワーカーが協調動作し、待ち時間を最小化。

- **リアルタイムUI**  
  各ワーカーの処理状況（Lines / Hits / Errors）を常時更新表示。長時間処理でも進捗が一目瞭然。

- **低メモリで巨大データ対応**  
  ストリーミング処理により、数百GBのデータセットでも安定動作。メモリ使用量は一定。

- **柔軟な抽出設定**  
  - 複数キーワードの OR 検索  
  - 文字数による絞り込み（最小 / 最大）  
  - 目標件数での自動停止  

---

## 🎯 他ツールとの使い分け

| ツール | 用途 | 特徴 |
|--------|------|------|
| **JPCC-PICKER（精密版）** | 表記ゆれを含めて漏れなく収集 | 全JSON解析で確実 / NFKC正規化 / 3つのモード対応 |
| **JPCC-RAPID-PICKER（高速版）** | とにかく速く大量収集したい | 行バイト検索で高速 / ヒット行のみJSON解析 / シンプル設計 |
| **JPCC-RANDOM-PICKER（本ツール）** | 統計的サンプル収集 | 真のランダム抽出 / パイプライン並列処理 / リアルタイムUI |

➡️ 研究・調査で統計的に有意なサンプルが必要なら、このツールが最適です。

---

## 🔧 技術的な仕組み

### パイプラインアーキテクチャ
```
[S3リスト] → [ファイルシャッフル] → [キューイング]
             ↓
      [Downloader群（並列）]
             ↓
         [Line Queue]
             ↓
       [Worker群（並列）]
             ↓
         [CSV書き込み]
```

### ランダム性の担保
1. **ファイル間**: 毎回異なる順序でファイルを処理（シード値で再現可能）  
2. **ファイル内**:  
   - 非圧縮（.jsonl）：S3 Range リクエストで真のランダム seek  
   - 圧縮（.jsonl.gz）：ストリーミング解凍中にランダム行数スキップ  

### メモリ効率
- ダウンロード済みデータは Queue で管理（最大20,000行）  
- 処理済みデータは即座に破棄  
- 重複チェックは SHA1 ハッシュで省メモリ化  

---

## 📥 インストール

```bash
git clone https://github.com/AI-NOSUKE/JPCC-RANDOM-PICKER.git
cd JPCC-RANDOM-PICKER
pip install -r requirements.txt
```

依存パッケージ:
- `boto3`: AWS S3 アクセス
- `orjson`（推奨）: 高速JSON処理。なくても動作しますが、あると約2倍高速化  

推奨インストール:
```bash
pip install boto3 orjson
```

---

## ▶️ 基本的な使い方

`jpcc-random-picker.py` の冒頭にある設定を編集：

```python
CONFIG = {
    "keywords": ["ももクロ", "ももいろクローバーZ"],  # OR検索
    "limit": 10000,                   # 収集目標件数
    "outfile": "output.csv",          # 出力ファイル
    "min_len": 100,                   # 最小文字数
    "max_len": 2000,                  # 最大文字数
    "seed": 42,                       # ランダムシード（再現性）
    "num_downloaders": 2,             # 同時ダウンロード数
    "processes": cpu_count(),         # CPU並列数（自動）
    "chunk_size": 2000,               # 1バッチ行数
    "bucket": "abeja-cc-ja",          # バケット名
    "max_gz_skip": 500_000,           # gzip時の最大スキップ行数
}
```

実行:
```bash
python jpcc-random-picker.py
```

---

## 🎛 詳細設定ガイド

| 設定項目 | 推奨値 | 説明 |
|----------|--------|------|
| `num_downloaders` | 2–4 | ネットワーク帯域に応じて調整。多すぎると S3 制限に注意 |
| `processes` | CPU数 | 通常は自動設定で最適。メモリ不足時は減らす |
| `chunk_size` | 2000 | 大きくすると効率的だが、レスポンスが悪くなる |
| `max_gz_skip` | 500,000 | gzipファイルのランダム性。大きいほどランダムだが初期化が遅い |

---

## 📊 実行時の画面例

```
=== JPCC Random Picker (Pipeline Mode) ===
  FILE PROGRESS: 42 / 1234
  [WORKER 00] | Lines:  123,456 | Hits:   543 | Errors:  2
  [WORKER 01] | Lines:  234,567 | Hits:   678 | Errors:  0
  [WORKER 02] | Lines:  345,678 | Hits:   890 | Errors:  1
  [WORKER 03] | Lines:  456,789 | Hits: 1,234 | Errors:  0

--- Logs ---
  [12:34:56] STEP3: パイプライン開始 (Downloader:2, Worker:4)
  [12:35:23] Download err: ReadTimeoutError
  [12:36:45] 処理中: s3://abeja-cc-ja/2021/cc-2021-04.jsonl.gz
----------------------------------
🎯 目標: 3,345 / 10,000 件
```

---

## 📂 出力形式

CSV（`output.csv`）:

```
id,text,char_len
a1b2c3d4e5f6g7h8,ももクロのライブは本当に楽しい！会場の一体感が素晴らしかった。,33
z9y8x7w6v5u4t3s2,ももいろクローバーZの新曲がリリースされました。今回も期待を裏切らない出来。,40
```

- `id`: ドキュメントID（存在しない場合はハッシュ値）  
- `text`: 改行を空白に置換した本文  
- `char_len`: 文字数  
- SHA1 ハッシュによる重複除去済み  

---

## 🚀 使用例

### 大規模な言説調査
```python
CONFIG = {
    "keywords": ["ChatGPT", "生成AI", "人工知能"],
    "limit": 100000,
    "min_len": 200,
    "max_len": 5000,
    "num_downloaders": 4,
}
```

### 特定商品の評判分析
```python
CONFIG = {
    "keywords": ["iPhone", "アイフォン"],
    "limit": 50000,
    "min_len": 50,
    "max_len": 1000,
    "seed": 12345,
}
```

### 地域情報の収集
```python
CONFIG = {
    "keywords": ["渋谷", "原宿", "表参道"],
    "limit": 20000,
    "min_len": 100,
    "max_len": 2000,
    "max_gz_skip": 1000000,
}
```

---

## ❓ Q&A

**Q. なぜ「真のランダム」なのですか？**  
A. ファイル順シャッフル + ファイル内ランダム seek で、データセット全体から均等にサンプリング。

**Q. どれくらい高速ですか？**  
A. 環境により異なりますが、毎秒 1,000〜5,000 行。10 万件なら 1〜2 時間程度。

**Q. メモリ不足になりませんか？**  
A. ストリーミング処理のため、通常 1〜2GB 程度で安定。

**Q. 同じ設定で実行すると同じ結果になりますか？**  
A. `seed` が同じなら再現可能。

**Q. エラーが出た場合は？**  
A. ネットワークエラーは自動リトライ。少数なら問題なし。

---

## 📜 ライセンス

このリポジトリは **JPCC-Limited-License（独自ライセンス）** に基づいて提供されています。MITなどのOSSライセンスではなく、**商用利用・再配布に制限があります**。  
※ JPCC データセット自体の利用には、データ提供者（ABEJA / Common Crawl）の利用規約が適用されます。

📘 **必ずご確認ください：** [`docs/License_FAQ_JA.md`](docs/License_FAQ_JA.md)

---

## 🤝 貢献

- Issue や Pull Request を歓迎します  
- バグ報告、機能提案、ドキュメント改善など、どんな貢献も大歓迎  

---

## 🔗 関連プロジェクト

- [JPCC-PICKER](https://github.com/AI-NOSUKE/JPCC-PICKER) – 精密版（表記ゆれ対応）
- [JPCC-RAPID-PICKER](https://github.com/AI-NOSUKE/JPCC-RAPID-PICKER) – 高速版（シンプル設計）
- [ABEJA-CC-JA – データセット公式情報](https://aws.amazon.com/marketplace/pp/prodview-nh5gwwr7rhqrq)  （AWS Marketplace / ABEJA公式公開ページ）
