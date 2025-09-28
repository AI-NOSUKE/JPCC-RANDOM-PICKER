JPCC-RANDOM-PICKER

巨大データセットから「真にランダムな」サンプルを高速抽出する、次世代型ピッカーです。
Japanese Common Crawl (JPCC) データセット（2019–2023年）から、キーワードを含む文章を偏りなくランダム抽出できます。

✨ 特徴
• 🎲 真のランダム性を実現
ファイル順をシャッフル + ファイル内でもランダム開始位置を選択。データセット全体から偏りのないサンプルを取得。
• ⚡ パイプライン処理で超高速
ダウンロード・検索・保存を並列化。複数のダウンローダーとワーカーが協調動作し、待ち時間を最小化。
• 📊 リアルタイムUI
各ワーカーの処理状況（Lines / Hits / Errors）を常時更新表示。長時間処理でも進捗が一目瞭然。
• 💾 低メモリで巨大データ対応
ストリーミング処理により、数百GBのデータセットでも安定動作。メモリ使用量は一定。
• 🔧 柔軟な抽出設定
o 複数キーワードのOR検索
o 文字数による絞り込み（最小/最大）
o 目標件数での自動停止

🎯 他ツールとの使い分け
ツール用途特徴JPCC-PICKER<br>（精密版）表記ゆれを含めて<br>漏れなく収集したい・全JSON解析で確実<br>・NFKC正規化<br>・3つのモード対応JPCC-RAPID-PICKER<br>（高速版）とにかく速く<br>大量収集したい・行バイト検索で高速<br>・ヒット行のみJSON解析<br>・シンプル設計JPCC-RANDOM-PICKER<br>（本ツール）偏りのない<br>統計的サンプル収集・真のランダム抽出<br>・パイプライン並列処理<br>・リアルタイムUI➡️ 研究・調査で統計的に有意なサンプルが必要なら、このツールが最適です。

🔧 技術的な仕組み
パイプラインアーキテクチャ
[S3リスト] → [ファイルシャッフル] → [キューイング]
                ↓
        [Downloader群（並列）]
                ↓
          [Line Queue]
                ↓
         [Worker群（並列）]
                ↓
           [CSV書き込み]
ランダム性の担保
1. ファイル間: 毎回異なる順序でファイルを処理（シード値で再現可能）
2. ファイル内: 
o 非圧縮(.jsonl): S3 Range リクエストで真のランダムseek
o 圧縮(.jsonl.gz): ストリーミング解凍中にランダム行数スキップ
メモリ効率
• ダウンロード済みデータは Queue で管理（最大20,000行）
• 処理済みデータは即座に破棄
• 重複チェックは SHA1 ハッシュで省メモリ化

📥 インストール
git clone https://github.com/AI-NOSUKE/JPCC-RANDOM-PICKER.git
cd JPCC-RANDOM-PICKER
pip install -r requirements.txt
依存パッケージ:
• boto3: AWS S3 アクセス
• orjson（推奨）: 高速JSON処理。なくても動作しますが、あると約2倍高速化
# orjson も含めてインストール（推奨）
pip install boto3 orjson

▶️ 基本的な使い方
jpcc-random-picker.py の冒頭にある設定を編集：
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
実行：
python jpcc-random-picker.py

🎛 詳細設定ガイド
パフォーマンスチューニング
設定項目推奨値説明num_downloaders2-4ネットワーク帯域に応じて調整。多すぎるとS3制限に引っかかるprocessesCPU数通常は自動設定で最適。メモリ不足時は減らすchunk_size2000大きくすると効率的だが、レスポンスが悪くなるmax_gz_skip500,000gzipファイルのランダム性。大きいほどランダムだが初期化が遅いメモリ使用量の目安
• 最小構成（1GB RAM）: num_downloaders=1, processes=2
• 標準構成（4GB RAM）: num_downloaders=2, processes=4
• 高速構成（8GB+ RAM）: num_downloaders=4, processes=8

📊 実行時の画面
=== JPCC Rapid Picker (Pipeline Mode) ===
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

📂 出力形式
id,text,char_len
a1b2c3d4e5f6g7h8,ももクロのライブは本当に楽しい！会場の一体感が素晴らしかった。,33
z9y8x7w6v5u4t3s2,ももいろクローバーZの新曲がリリースされました。今回も期待を裏切らない出来。,40
• id: ドキュメントID（存在しない場合はテキストのハッシュ値）
• text: 改行を空白に置換した本文
• char_len: 文字数
• 重複除去済み（SHA1ハッシュによる）

🚀 使用例
例1: 大規模な言説調査
CONFIG = {
    "keywords": ["ChatGPT", "生成AI", "人工知能"],
    "limit": 100000,      # 10万件のサンプル
    "min_len": 200,       # ある程度の長さの文章のみ
    "max_len": 5000,
    "num_downloaders": 4,  # 高速収集
}
例2: 特定商品の評判分析
CONFIG = {
    "keywords": ["iPhone", "アイフォン"],
    "limit": 50000,
    "min_len": 50,        # 短いコメントも含める
    "max_len": 1000,      # 長文レビューは除外
    "seed": 12345,        # 再現可能な抽出
}
例3: 地域情報の収集
CONFIG = {
    "keywords": ["渋谷", "原宿", "表参道"],
    "limit": 20000,
    "min_len": 100,
    "max_len": 2000,
    "max_gz_skip": 1000000,  # より深くランダム化
}

❓ Q&A
Q. なぜ「真のランダム」なのですか？
A. 通常の逐次処理では、データセットの前半に偏りがちです。本ツールは、ファイル順のシャッフルとファイル内のランダムseekにより、データセット全体から均等にサンプリングします。
Q. どれくらい高速ですか？
A. 環境により異なりますが、標準的な構成で毎秒1,000〜5,000行を処理できます。10万件の収集なら1〜2時間程度です。
Q. メモリ不足になりませんか？
A. ストリーミング処理のため、データセットのサイズに関わらずメモリ使用量は一定（通常1〜2GB程度）です。
Q. 同じ設定で実行すると同じ結果になりますか？
A. seed値が同じなら、ランダム性は再現可能です。研究の再現性を保証できます。
Q. エラーが出た場合は？
A. ネットワークエラーは自動リトライされます。UI上でエラー数が確認できますが、少数なら問題ありません。

🔬 技術詳細（上級者向け）
S3 Range リクエストの活用
非圧縮JSONLファイルに対しては、HTTPのRangeヘッダーを使用してファイルの任意位置から読み込みを開始。これにより、巨大ファイルでも先頭から読む必要がなく、真のランダムアクセスを実現。
start_pos = rng.randint(0, size-1)
rng_hdr = f"bytes={start_pos}-{size-1}"
obj = s3.get_object(Bucket=BUCKET, Key=key, Range=rng_hdr)
マルチプロセスの初期化
各ワーカープロセスは独自の正規表現パターンを持ち、起動時に一度だけコンパイル。プロセス間通信はQueueを使用し、オーバーヘッドを最小化。
重複検出の最適化
SHA1ハッシュによる重複検出。完全一致ではなくハッシュ値で比較することで、メモリ使用量を大幅に削減（1件あたり20バイト）。

📜 ライセンス
MIT License のもとで自由に利用可能です。
注意: 本ツールのライセンスはソフトウェアのみに適用されます。
JPCC データセット自体の利用には、データ提供者（ABEJA / Common Crawl）の利用規約が適用されます。

🤝 貢献
Issue や Pull Request を歓迎します！
バグ報告、機能提案、ドキュメント改善など、どんな貢献も大歓迎です。

🔗 関連プロジェクト
• JPCC-PICKER - 精密版（表記ゆれ対応）
• JPCC-RAPID-PICKER - 高速版（シンプル設計）
• ABEJA-CC-JA - データセット公式情報

