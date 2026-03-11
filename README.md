# 花王CPレシートチェッカー v2

花王ビオレキャンペーンの応募レシートを処理するシステムです。
レシート画像をAIで読み取り、作業者が確認・入力し、当選者を抽選して納品データを作成します。

## このシステムでできること

1. **レシート画像の自動読み取り（OCR）** - AIがレシートから店舗名・商品名・金額を読み取ります
2. **作業者による確認・入力** - AIの読み取り精度が低い場合は、作業者が手入力で補完します
3. **抽選** - 条件を満たした応募者から当選者を自動で選びます
4. **納品データ出力** - 花王指定のCSVフォーマットでデータを出力します

## 仕組み

```
応募データ（CSV + レシート画像）
    ↓
① データ取り込み（import_csv.py）
    ↓
② AI読み取り（reocr_all.py）
    ↓  信頼度90%以上 → 自動確定
    ↓  信頼度90%未満 → 作業者が確認
    ↓
③ 作業者が入力画面で確認・修正（app.py）
    ↓
④ 管理者が品質チェック（10〜15%を抜き打ち）
    ↓
⑤ 抽選＆納品データ出力（pipeline.py）
```

## 作業画面（Streamlit）

https://kao-cp-receipt-checker2-svebtpk4vdxpdb5dwxazqb.streamlit.app/

- ログイン後、対象の賞（A賞、B賞など）を選択
- 担当者欄で自分の名前を設定
- レシート画像を見ながらデータを入力
- 複数人で同時作業OK（同じレシートを2人が開かないようロックがかかります）

## 対象の賞

| 賞 | 対象金額 | 当選数 | 予備 | 納品日 |
|----|----------|--------|------|--------|
| S賞 | 10,000円以上 | 2組 | 3組 | 3/17 |
| A賞 | 1,700円以上 | 25組 | 10組 | 3/17 |
| B賞 | 1,700円以上 | 25組 | 10組 | 3/17 |
| C賞 | 600円以上 | 1,000名 | 50名 | 3/24 |
| SP（ツルハ） | 600円以上 | 500名 | 20名 | 3/24 |
| SP（ウエルシア） | 600円以上 | 500名 | 20名 | 3/24 |

## ファイル構成

```
├── app.py                 ← 作業画面（Streamlit）
├── config.py              ← 賞の設定・各種定数
├── firebase_client.py     ← データベース操作（Firebase）
├── ocr.py                 ← AI読み取り（Gemini）
├── pipeline.py            ← 抽選・CSV出力の実行
├── import_csv.py          ← 応募データの取り込み
├── product_master.py      ← 商品名の照合（あいまい検索）
├── upload_images.py       ← レシート画像の一括アップロード
├── reocr_all.py           ← AI読み取りの一括再実行
├── requirements.txt       ← 必要なライブラリ一覧
├── data/                  ← 応募データ・商品マスター
│   ├── A賞/              ← A賞の応募CSVとレシート画像
│   ├── B賞/              ← B賞の応募CSVとレシート画像
│   ├── cp_target_master.json    ← CP対象商品リスト
│   └── kao_other_master.json    ← その他花王製品リスト
└── scripts/               ← 過去に使った一回限りのスクリプト
    ├── migrate_json_to_firestore.py  ← 旧データの移行用
    ├── migrate_split_receipts.py     ← データ構造の変換用
    └── test_store_ocr.py             ← 店舗名読み取りテスト用
```

## 運用の流れ

### 1. データ取り込み

応募データ（CSV）とレシート画像をシステムに登録します。

```bash
python import_csv.py --campaign kao_cp_2026 --prize A --csv data/A賞/応募データ.csv --images data/A賞/
```

### 2. AI読み取り（OCR）

登録した画像をAIで一括読み取りします。

```bash
python reocr_all.py --campaign kao_cp_2026 --prize A
```

### 3. 作業者が入力

作業画面（Streamlit）を開いて、AIが読み取れなかったレシートを手入力します。

```bash
streamlit run app.py
```

※ 本番環境はStreamlit Cloudにデプロイ済みなので、URLから直接アクセスできます。

### 4. 抽選・CSV出力

入力が完了したら、抽選を実行して納品CSVを出力します。

```bash
python pipeline.py --campaign kao_cp_2026 --prize A --action lottery
python pipeline.py --campaign kao_cp_2026 --prize A --action export_csv
```

## セットアップ（開発者向け）

### 必要なもの

- Python 3.11以上
- Firebaseプロジェクト（Firestore + Cloud Storage）
- Google Gemini APIキー

### 手順

```bash
# ライブラリのインストール
pip install -r requirements.txt

# Firebase認証キーを配置
# .secrets/serviceAccountKey.json にFirebaseのサービスアカウントキーを置く

# Streamlit設定
# .streamlit/secrets.toml にFirebase認証情報を記載
```

## 入力ルール（作業者向け）

- **税抜きレシート**: 金額 × 110% で入力（例: 880円 → 968円）
- **クーポン割引**: 割引前の金額で入力
- **イトーヨーカドー**: 「花王ビオレ」表記でも602円以上なら対象商品として記載
- **CP対象外の花王製品**（アタック、メリット、キュレル等）: 「その他花王製品」欄に入力
- **判断に迷う場合**: 「CP対象か判断つかず」欄に入力
