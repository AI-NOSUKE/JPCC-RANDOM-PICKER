# JPCC-RANDOM-PICKER

[![CI](https://github.com/AI-NOSUKE/JPCC-RANDOM-PICKER/actions/workflows/ci.yml/badge.svg)](https://github.com/AI-NOSUKE/JPCC-RANDOM-PICKER/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: Custom](https://img.shields.io/badge/license-custom-lightgrey.svg)](LICENSE)

ABEJA-CC-JA の公開 S3 バケットにある日本語 Common Crawl 由来 JSONL から、指定キーワードを含む文章をランダム性を高めて抽出し、CSV に保存する Python ツールです。

この版は `v1.4-best-abeja` として、データソースを ABEJA-CC-JA 専用に整理しています。巨大な全件データをローカルへ落とさず、S3 上の JSONL / JSONL.GZ を部分的に読みながら候補を集めます。

## 特徴

- 複数キーワードの OR 検索
- 対話モードとコマンドライン引数の両対応
- 非圧縮 JSONL は S3 Range でランダムなウィンドウだけを読み込み
- gzip JSONL は疑似ランダム行スキップで部分的に読み込み
- NFKC 正規化、casefold、半角カナ、全角英数などを考慮したキーワード判定
- ダウンロード、JSON 解析、キーワード検索を並列処理
- 指定件数より多めに候補を見て、決定的乱数スコアで最終件数に絞り込み
- 実行時間上限に達したとき、件数未達なら延長確認
- 出力 CSV は `id`, `url`, `text`, `char_len`

## 注意

このツールは、日本語生活者全体や SNS 発話全体の代表サンプルを作るものではありません。抽出対象は Common Crawl に捕捉され、ABEJA-CC-JA の前処理を通った日本語 Web テキストです。

全件走査による厳密な一様抽出ではなく、速度、低メモリ、ランダムな部分観測のバランスを優先しています。Web 由来のため、広告文、SEO 記事、企業ページ、商品説明、崩れた本文が混ざる可能性があります。

## 必要環境

- Python 3.11 以上を推奨
- インターネット接続
- 依存パッケージ: `boto3`, `orjson`

`boto3` は ABEJA-CC-JA の S3 読み込みに必要です。`orjson` は高速 JSON 解析用です。未導入の場合は標準の `json` にフォールバックしますが、通常はインストールを推奨します。

## インストール

```bash
git clone https://github.com/AI-NOSUKE/JPCC-RANDOM-PICKER.git
cd JPCC-RANDOM-PICKER
python -m pip install -r requirements.txt
```

## 使い方

対話モード:

```bash
python jpcc-random-picker.py
```

実行すると、検索キーワードと抽出件数を聞かれます。複数キーワードはスペース、カンマ、読点で区切ると OR 検索になります。

引数モード:

```bash
python jpcc-random-picker.py -k ももクロ ももいろクローバーZ -n 1000 -o output.csv
```

時間上限や候補確認数を指定する例:

```bash
python jpcc-random-picker.py \
  -k ChatGPT 生成AI 人工知能 \
  -n 5000 \
  -o ai_comments.csv \
  --max-minutes 20 \
  --oversample-factor 3
```

### オプション

| オプション | 説明 | 既定値 |
| --- | --- | --- |
| `-k`, `--keywords` | 検索キーワード。複数指定で OR 検索 | 対話入力 |
| `-n`, `--limit` | 最終出力件数 | 対話入力 |
| `-o`, `--outfile` | 出力 CSV ファイル名 | `output.csv` |
| `--max-minutes` | 時間上限。超えたら途中結果で打ち切り、件数未達なら延長確認 | `15` |
| `--oversample-factor` | 指定件数の何倍まで候補を確認してから絞るか。速度優先なら `1` | `3.0` |

## 出力形式

CSV は UTF-8 で保存されます。

```csv
id,url,text,char_len
https://example.com/article,https://example.com/article,本文...,312
```

| 列 | 内容 |
| --- | --- |
| `id` | 元データの `id`、なければ `url`、どちらもなければ本文ハッシュ由来の ID |
| `url` | 元データの URL。存在しない場合は空欄 |
| `text` | 抽出本文。改行は空白に置換 |
| `char_len` | 本文の文字数 |

同一本文は SHA1 ハッシュで重複除去されます。

## 高度な設定

通常は CLI オプションだけで使えます。細かく調整したい場合は、`jpcc-random-picker.py` 冒頭の `CONFIG` を編集します。

| 設定 | 内容 |
| --- | --- |
| `min_len` / `max_len` | 抽出対象にする本文文字数の下限・上限 |
| `seed` | ランダムシード。再現性に使います |
| `num_downloaders` | S3 読み込みスレッド数 |
| `processes` | JSON 解析・判定に使うプロセス数 |
| `chunk_size` | worker に渡す行数の単位 |
| `window_bytes` | 非圧縮 JSONL で 1 回に読む Range サイズ |
| `windows_per_file` | 非圧縮 JSONL で 1 ファイルあたり読むランダムウィンドウ数 |
| `max_lines_per_window` | 1 ウィンドウで読む最大行数 |
| `gz_windows_per_file` | gzip JSONL で 1 ファイルあたり読む回数 |
| `max_gz_skip` | gzip JSONL で先頭から疑似ランダムに飛ばす最大行数 |
| `max_passes` | 件数未達時にウィンドウ位置を変えて再走査する最大回数 |

## 仕組み

1. ABEJA-CC-JA の S3 バケットから `.jsonl` / `.jsonl.gz` の一覧を取得
2. ファイル順をシード付きでシャッフル
3. 非圧縮 JSONL はランダムなバイト範囲を読み、壊れていない JSONL 行だけを使う
4. gzip JSONL は先頭からランダム行数をスキップし、その後の一定行を読む
5. bytes 事前フィルタで候補を絞り、JSON 解析後に NFKC + casefold 済み本文で最終判定
6. 重複本文を除き、候補ごとに決定的乱数スコアを付ける
7. `limit * oversample_factor` 件まで候補を確認し、スコアで指定件数に絞って CSV 保存

## トラブルシューティング

### `boto3が未インストールです` と表示される

依存パッケージをインストールしてください。

```bash
python -m pip install -r requirements.txt
```

### 途中結果しか保存されない

`--max-minutes` の時間上限に達した可能性があります。件数未達時は延長確認が表示されます。無応答の場合は自動終了します。

### ヒット数が少ない

キーワードが ABEJA-CC-JA に少ない、文字数条件に合わない、ランダムに読んだ範囲に十分含まれなかった、などが考えられます。`--max-minutes` や `--oversample-factor`、`CONFIG["max_passes"]` を増やすと見つかる可能性があります。

## ライセンス

このリポジトリは独自ライセンスで提供されています。詳細は [`LICENSE`](LICENSE) と [`docs/License_FAQ_JA.md`](docs/License_FAQ_JA.md) を確認してください。

ABEJA-CC-JA および Common Crawl 由来データの利用条件は、各データ提供元の規約に従ってください。
