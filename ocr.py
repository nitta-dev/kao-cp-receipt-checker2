"""
花王CP レシートチェックv2 - OCR処理（信頼度スコア付き）
Gemini 2.5 Flash APIを使用
"""
import base64
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from config import CONFIDENCE_THRESHOLD

GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}

MAX_RETRIES = 3

OCR_PROMPT = """
このレシート画像を分析し、JSON形式で情報を抽出してください。

## 抽出項目
- store_name: 店舗チェーン名（レシートに記載されている通りに）
- store_branch: 支店名
- purchase_date: 購入日時（YYYY-MM-DD HH:MM形式、読み取れない場合はnull）
- tax_type: レシート全体の税表示方式を判定
  - "inclusive": 金額の横に「内」がある、または小計と合計が一致 → 税込価格
  - "exclusive": 「外税」表記がある、または小計＜合計 → 税抜価格
  - ★Amazon適格請求書の場合: 「価格 税抜」と「小計 税込」の両列がある。priceには「小計 税込」列の金額を使い、tax_typeは"inclusive"とすること
- tax_rate_10_subtotal: 10%対象の小計（レシートに記載があれば。なければnull）
- tax_rate_8_subtotal: 8%（軽減税率）対象の小計（レシートに記載があれば。なければnull）
- items: 商品リスト（全商品）
  - name: 商品名（★レシートに印字されている通りそのまま記載。省略表記はそのまま。例: 「ビTBととのいフ」→ そのまま「ビTBととのいフ」。正式名称に展開しない）
  - price: 金額（整数、レシートに記載されている金額そのまま。値引き行は別itemとしてマイナス値で記録）
  - tax_label: その商品行に「内」「※」「＊」等の税関連マークがあればその文字。なければnull
  - is_kao: 花王製品と思われるならtrue（ビオレ、アタック、メリット、キュレル等）
  - is_cp_target: 以下の3シリーズ「のみ」が対象。それ以外のビオレ製品はfalse:
    - 「ビオレ ザ ボディ」「ビオレu ザ ボディ」系（泡・液体・ジェル・乳液・シャワーヘッド含む）
    - 「ビオレ ザ ハンド」系（泡ハンドソープ・ハンド乳液含む）
    - 「ととのい肌」シリーズ
    - 省略表記の判定ガイド: ビTB=ビオレ ザ ボディ、ビTH=ビオレ ザ ハンド、THS/HHS=ザ ハンド ソープ
    - ★対象外の例: 「ビオレ ザ クレンズ」「ビオレu 泡ハンドソープ（ザ ハンドではない通常版）」「ビオレu 泡ボディウォッシュ（ザ ボディではない通常版）」「メイク落とし」「洗顔」等はfalse
    - ★「花王ビオレ」「ビオレ」のみで具体的なシリーズ名が読めない場合 → is_cp_target=false, confidence 0.70以下
    - ★迷ったらis_cp_target=falseにして、「判断つかず」として人間に委ねる
- subtotal: 小計金額（整数）
- total: 合計金額（整数、税込の最終支払額）
- confidence: CP対象品の判定信頼度（0.0〜1.0）。★最も重要な軸は「is_cp_targetの判定が正しいと言い切れるか」
  - 1.0: CP対象品が明確に特定できる。「ビオレ ザ ボディ ととのい肌」等の正式名称が読める。迷いゼロ
  - 0.95: 省略表記だがCP対象品を確信できる。「ビTBととのい」「ビオレボディ整泡」等の既知パターン
  - 0.90: CP対象品の判定はできたが、省略が深くやや迷った。または花王製品か否かの判断に軽微な迷い
  - 0.80: CP対象品がありそうだが、省略や画像不鮮明で判定に自信がない。人間確認推奨
  - 0.70: 花王製品らしきものがあるが、CP対象の3シリーズか通常版かの区別がつかない
  - 0.60以下: 商品名が読めない、またはレシートとして判別困難
  - 0.00: レシート画像ではない、または全く読めない
  - ★CP対象品が0件のレシートでも、全商品を読み取った上で「対象品なし」と確信できれば0.90〜0.95（AUTOにする）。読めなくて0件なのか、本当に0件なのかを区別すること

## 信頼度の判定基準
- ベース: 0.5からスタート
- 店舗名・日付・金額が読める → +0.1
- 全商品名が読める（省略でもOK） → +0.1
- is_cp_targetの判定に迷いなし → +0.2（最重要）
- is_kao（花王製品）の判定に迷いなし → +0.1
- 上記すべて満たす → +0.05
- ★省略表記（ビTB、ビTH、THS等）はPOS業界標準。正しく読めていれば減点不要
- 画像不鮮明で商品名が読めない → -0.1〜-0.3
- CP対象の3シリーズと通常版ビオレの区別に迷い → -0.15
- 「ザ ボディ」なのか「泡ボディウォッシュ（通常版）」なのか不明 → -0.2
- 金額の読み取りに推測 → -0.05
- ★★重要★★「花王ビオレ」「ビオレu」「ビオレ」のみで具体的なシリーズ名（ザ ボディ / ザ ハンド / ととのい肌）が読めない商品がある → confidenceを0.70以下にすること。is_cp_target=falseにしていても、人間がレシート原本で確認すべき。この場合confidenceが0.90以上になることは絶対にない

## 絶対に守ること（嘘をつかない）
- ★読み取れない文字を推測で埋めない。読めなければnullにする
- ★商品名はレシート印字のまま転記すること。省略表記を正式名称に展開しない
- ★自信がないのに高いconfidenceをつけない。迷ったら低くする。人間チェックに回す方が安全
- ★存在しない商品を捏造しない。見えない行を「たぶんあるだろう」で追加しない
- ★「内」マークの有無を正確に読み取ること（税込/税抜の判定に直結）
- ★is_cp_targetは3シリーズのみ。「ビオレ ザ ○○」が全て対象ではない。「ザ クレンズ」等は対象外
- ★1文字でも省略・脱落させない。レシートの印字を1文字ずつ正確に転記
- ★Amazon/EC系: 税抜と税込の両方の列がある場合は、必ず「税込」の金額を使う
- 金額は数値のみ（カンマなし）
- 読み取れない箇所はnull
- is_cp_target の判定に迷う場合はfalseにして、confidenceを下げる

## 出力（JSONのみ、説明文不要）
```json
{
  "store_name": "...",
  "store_branch": "...",
  "purchase_date": "YYYY-MM-DD HH:MM",
  "tax_type": "inclusive",
  "tax_rate_10_subtotal": null,
  "tax_rate_8_subtotal": null,
  "items": [
    {"name": "...", "price": 0, "tax_label": "内", "is_kao": false, "is_cp_target": false}
  ],
  "subtotal": 0,
  "total": 0,
  "confidence": 0.85
}
```
"""


def get_api_key() -> str:
    key = os.environ.get("GEMINI_API_KEY", "")
    if not key:
        raise RuntimeError("環境変数 GEMINI_API_KEY が未設定です")
    return key


def _adjust_tax(parsed: dict) -> dict:
    """
    税抜価格を税込に補正する。

    ロジック:
    1. tax_type == "inclusive" or 小計==合計 → 何もしない
    2. tax_type == "exclusive" or 小計<合計 → 各商品に消費税を加算
       - tax_label に「※」「＊」がある商品 → 8%（軽減税率・食品）
       - それ以外 → 10%
    """
    tax_type = parsed.get("tax_type", "inclusive")
    subtotal = parsed.get("subtotal") or 0
    total = parsed.get("total") or 0

    # 税込判定: 明示的にinclusive、または小計と合計が一致
    if tax_type == "inclusive":
        return parsed
    if subtotal > 0 and total > 0 and subtotal == total:
        parsed["tax_type"] = "inclusive"
        return parsed

    # 税抜 → 税込補正
    for item in parsed.get("items", []):
        price = item.get("price")
        if price is None or price <= 0:
            continue
        tax_label = item.get("tax_label") or ""
        # 軽減税率マーク（※, ＊, *）→ 8%
        if any(m in tax_label for m in ("※", "＊", "*")):
            item["price"] = math.ceil(price * 1.08)
            item["tax_adjusted"] = "8%"
        else:
            item["price"] = math.ceil(price * 1.1)
            item["tax_adjusted"] = "10%"

    parsed["tax_adjusted"] = True
    return parsed


def _parse_json_response(text: str) -> dict:
    """GeminiレスポンスからJSONを抽出。複数のパターンに対応。"""
    # ```json ... ``` ブロック
    if "```json" in text:
        json_str = text.split("```json")[1].split("```")[0]
        return json.loads(json_str.strip())
    if "```" in text:
        json_str = text.split("```")[1].split("```")[0]
        return json.loads(json_str.strip())
    # ブロックなし → テキスト全体をパース
    return json.loads(text.strip())


def ocr_single(image_path: str | Path) -> dict:
    """
    1枚のレシート画像をOCR処理し、信頼度スコア付きの結果を返す。
    エラー時は最大MAX_RETRIES回リトライする。
    """
    import requests

    image_path = Path(image_path)
    if not image_path.exists():
        return {"image_path": str(image_path), "error": f"ファイルが見つかりません: {image_path}"}

    # 画像をBase64エンコード
    with open(image_path, "rb") as f:
        image_data = base64.b64encode(f.read()).decode("utf-8")

    ext = image_path.suffix.lower()
    mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                ".gif": "image/gif", ".webp": "image/webp"}
    mime_type = mime_map.get(ext, "image/jpeg")

    api_key = get_api_key()
    payload = {
        "contents": [{
            "parts": [
                {"text": OCR_PROMPT},
                {"inline_data": {"mime_type": mime_type, "data": image_data}},
            ]
        }],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 4096,
        },
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                f"{GEMINI_API_URL}?key={api_key}",
                json=payload,
                timeout=60,
            )
            resp.raise_for_status()
            result = resp.json()

            text = result["candidates"][0]["content"]["parts"][0]["text"]
            parsed = _parse_json_response(text)

            # 税補正
            parsed = _adjust_tax(parsed)

            # CP対象品・花王製品の合計を自動計算
            cp_target_total = 0
            kao_other_total = 0
            for item in parsed.get("items", []):
                price = item.get("price") or 0
                if price < 0:
                    continue  # 値引き行はスキップ
                if item.get("is_cp_target"):
                    cp_target_total += price
                elif item.get("is_kao"):
                    kao_other_total += price

            confidence = parsed.get("confidence", 0.0)

            return {
                "image_path": str(image_path),
                "store_name": parsed.get("store_name"),
                "store_branch": parsed.get("store_branch"),
                "purchase_date": parsed.get("purchase_date"),
                "items": parsed.get("items", []),
                "tax_type": parsed.get("tax_type", "inclusive"),
                "tax_adjusted": parsed.get("tax_adjusted", False),
                "subtotal": parsed.get("subtotal"),
                "total": parsed.get("total", 0),
                "confidence": confidence,
                "cp_target_total": cp_target_total,
                "kao_other_total": kao_other_total,
                "is_auto": confidence >= CONFIDENCE_THRESHOLD,
                "error": None,
            }

        except (requests.RequestException, json.JSONDecodeError, KeyError, IndexError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)  # 2s, 4s のバックオフ
                continue

    return {
        "image_path": str(image_path),
        "error": f"リトライ{MAX_RETRIES}回失敗: {last_error}",
    }


def ocr_single_from_bytes(image_bytes: bytes, filename: str = "image.jpg") -> dict:
    """
    バイト列から1枚のレシート画像をOCR処理（Firebase Storage経由用）。
    """
    import requests

    image_data = base64.b64encode(image_bytes).decode("utf-8")

    ext = Path(filename).suffix.lower()
    mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                ".gif": "image/gif", ".webp": "image/webp"}
    mime_type = mime_map.get(ext, "image/jpeg")

    api_key = get_api_key()
    payload = {
        "contents": [{
            "parts": [
                {"text": OCR_PROMPT},
                {"inline_data": {"mime_type": mime_type, "data": image_data}},
            ]
        }],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 4096,
        },
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                f"{GEMINI_API_URL}?key={api_key}",
                json=payload,
                timeout=60,
            )
            resp.raise_for_status()
            result = resp.json()

            text = result["candidates"][0]["content"]["parts"][0]["text"]
            parsed = _parse_json_response(text)
            parsed = _adjust_tax(parsed)

            cp_target_total = 0
            kao_other_total = 0
            for item in parsed.get("items", []):
                price = item.get("price") or 0
                if price < 0:
                    continue
                if item.get("is_cp_target"):
                    cp_target_total += price
                elif item.get("is_kao"):
                    kao_other_total += price

            confidence = parsed.get("confidence", 0.0)

            return {
                "filename": filename,
                "store_name": parsed.get("store_name"),
                "store_branch": parsed.get("store_branch"),
                "purchase_date": parsed.get("purchase_date"),
                "items": parsed.get("items", []),
                "tax_type": parsed.get("tax_type", "inclusive"),
                "tax_adjusted": parsed.get("tax_adjusted", False),
                "subtotal": parsed.get("subtotal"),
                "total": parsed.get("total", 0),
                "confidence": confidence,
                "cp_target_total": cp_target_total,
                "kao_other_total": kao_other_total,
                "is_auto": confidence >= CONFIDENCE_THRESHOLD,
                "error": None,
            }

        except (requests.RequestException, json.JSONDecodeError, KeyError, IndexError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)
                continue

    return {
        "filename": filename,
        "error": f"リトライ{MAX_RETRIES}回失敗: {last_error}",
    }


def ocr_batch(
    image_paths: list[str | Path],
    max_workers: int = 3,
    batch_interval: float = 5.0,
) -> list[dict]:
    """
    複数画像を並列バッチ処理。

    Args:
        image_paths: 画像ファイルパスのリスト
        max_workers: 同時実行数
        batch_interval: バッチ間の待機秒数（API rate limit対策）

    Returns:
        OCR結果のリスト（入力順）
    """
    results = [None] * len(image_paths)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i, path in enumerate(image_paths):
            future = executor.submit(ocr_single, path)
            futures[future] = i

            # バッチ間隔
            if (i + 1) % max_workers == 0 and i + 1 < len(image_paths):
                time.sleep(batch_interval)

        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                results[idx] = {
                    "image_path": str(image_paths[idx]),
                    "error": str(e),
                }

    return results
