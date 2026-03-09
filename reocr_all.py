"""
B賞全件OCR再実行スクリプト
既存のconfidenceをリセットしてから新プロンプトでOCR再実行
"""
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# サービスアカウントキーの設定
os.environ.setdefault(
    "FIREBASE_CREDENTIALS",
    os.path.join(os.path.dirname(__file__), ".secrets", "serviceAccountKey.json"),
)

from firebase_client import init_firebase, get_entries, update_entry, download_image_bytes
from ocr import ocr_single_from_bytes
from config import CONFIDENCE_THRESHOLD, DEFAULT_CAMPAIGN_ID

CAMPAIGN = DEFAULT_CAMPAIGN_ID
PRIZE = "B"
WORKERS = 10


def main():
    init_firebase()
    entries = get_entries(CAMPAIGN, PRIZE)
    print(f"B賞エントリ数: {len(entries)}")

    # Step 1: 全件のconfidenceをリセット（人間入力済みは除く）
    reset_count = 0
    for entry in entries:
        entry_id = entry["_id"]
        if entry.get("human_input_done"):
            print(f"  #{entry.get('file_number', '?')} — 人間入力済み、スキップ")
            continue
        update_entry(CAMPAIGN, PRIZE, entry_id, {
            "confidence": None,
            "is_auto": False,
            "error": None,
        })
        reset_count += 1

    print(f"\n{reset_count}件のconfidenceをリセット完了")

    # Step 2: OCR再実行
    targets = [e for e in entries if not e.get("human_input_done")]
    print(f"OCR再実行対象: {len(targets)}件")

    if not targets:
        print("対象なし。終了。")
        return

    success = 0
    errors = 0
    completed = 0

    def _ocr_one(entry, max_retries=3):
        entry_id = entry["_id"]
        storage_path = entry.get("storage_path", "")
        filename = entry.get("original_filename", "?")
        file_num = entry.get("file_number", "?")

        for attempt in range(max_retries):
            try:
                img_bytes = download_image_bytes(storage_path)
                result = ocr_single_from_bytes(img_bytes, filename)

                update_data = {
                    "confidence": result.get("confidence"),
                    "is_auto": result.get("is_auto", False),
                    "error": result.get("error"),
                    "store_name": result.get("store_name"),
                    "store_branch": result.get("store_branch"),
                    "purchase_date": result.get("purchase_date"),
                    "items": result.get("items", []),
                    "tax_type": result.get("tax_type"),
                    "tax_adjusted": result.get("tax_adjusted", False),
                    "subtotal": result.get("subtotal"),
                    "total": result.get("total"),
                    "cp_target_total": result.get("cp_target_total", 0),
                    "kao_other_total": result.get("kao_other_total", 0),
                }
                update_entry(CAMPAIGN, PRIZE, entry_id, update_data)
                return ("ok", file_num, filename, result)
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                try:
                    update_entry(CAMPAIGN, PRIZE, entry_id, {"error": str(e)})
                except Exception:
                    pass
                return ("error", file_num, filename, str(e))

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(_ocr_one, entry): entry for entry in targets}
        for future in as_completed(futures):
            completed += 1
            status_type, file_num, filename, data = future.result()
            if status_type == "ok":
                conf = data.get("confidence", 0)
                label = "AUTO" if data.get("is_auto") else "MANUAL"
                print(f"  [{completed}/{len(targets)}] #{file_num} {filename}: conf={conf:.2f} -> {label}")
                success += 1
            else:
                print(f"  [{completed}/{len(targets)}] #{file_num} {filename}: ERROR - {data}")
                errors += 1

    # 結果サマリー
    print(f"\n=== OCR再実行完了 ===")
    print(f"処理: {success}件, エラー: {errors}件")

    # 最新の状態を取得
    updated_entries = get_entries(CAMPAIGN, PRIZE)
    auto_count = sum(1 for e in updated_entries if e.get("is_auto") and not e.get("human_input_done"))
    manual_count = sum(1 for e in updated_entries if not e.get("is_auto") and not e.get("human_input_done") and not e.get("error"))
    human_done = sum(1 for e in updated_entries if e.get("human_input_done"))
    error_count = sum(1 for e in updated_entries if e.get("error"))

    print(f"\nAI自動確定(>={CONFIDENCE_THRESHOLD:.0%}): {auto_count}件")
    print(f"要手入力(<{CONFIDENCE_THRESHOLD:.0%}): {manual_count}件")
    print(f"人間入力済み: {human_done}件")
    print(f"エラー: {error_count}件")


if __name__ == "__main__":
    main()
