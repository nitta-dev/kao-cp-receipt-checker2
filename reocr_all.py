"""
全件OCR再実行スクリプト
既存のconfidenceをリセットしてから新プロンプトでOCR再実行
新形式(images[]配列)・旧形式(トップレベルstorage_path)両対応
"""
import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# サービスアカウントキーの設定
os.environ.setdefault(
    "FIREBASE_CREDENTIALS",
    os.path.join(os.path.dirname(__file__), ".secrets", "serviceAccountKey.json"),
)

from firebase_client import init_firebase, get_entries, update_entry, download_image_bytes
from ocr import ocr_single_from_bytes
from config import CONFIDENCE_THRESHOLD, DEFAULT_CAMPAIGN_ID

WORKERS = 10


def _has_images(entry):
    """新形式(images[]配列あり)かどうか"""
    images = entry.get("images")
    return isinstance(images, list) and len(images) > 0


def main():
    parser = argparse.ArgumentParser(description="全件OCR再実行")
    parser.add_argument("--prize", required=True, help="賞種別 (A or B)")
    parser.add_argument("--campaign", default=DEFAULT_CAMPAIGN_ID, help="キャンペーンID")
    parser.add_argument("--workers", type=int, default=WORKERS, help="並列数")
    args = parser.parse_args()

    campaign = args.campaign
    prize = args.prize

    init_firebase()
    entries = get_entries(campaign, prize)
    print(f"{prize}賞エントリ数: {len(entries)}")

    # Step 1: 全件のconfidenceをリセット（人間入力済みは除く）
    reset_count = 0
    for entry in entries:
        entry_id = entry["_id"]
        if entry.get("human_input_done"):
            print(f"  #{entry.get('file_number', '?')} — 人間入力済み、スキップ")
            continue

        reset_data = {
            "confidence": None,
            "is_auto": False,
            "error": None,
        }

        # 新形式: images[]内のOCRフィールドもリセット
        if _has_images(entry):
            for img in entry["images"]:
                img["ocr_done"] = False
                img["confidence"] = None
                img["is_auto"] = False
                img["error"] = None
                img["store_name"] = None
                img["store_branch"] = None
                img["purchase_date"] = None
                img["items"] = []
                img["tax_type"] = None
                img["tax_adjusted"] = False
                img["subtotal"] = None
                img["total"] = None
                img["cp_target_total"] = 0
                img["kao_other_total"] = 0
            reset_data["images"] = entry["images"]
            reset_data["cp_target_total"] = 0

        update_entry(campaign, prize, entry_id, reset_data)
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

    def _ocr_one(entry, img_info=None, max_retries=3):
        """1件のOCR処理（スレッドセーフ・リトライ付き）"""
        entry_id = entry["_id"]
        file_num = entry.get("file_number", "?")

        if img_info:
            # 新形式: images配列内の1画像
            storage_path = img_info["storage_path"]
            filename = img_info.get("original_filename", "?")
        else:
            # 旧形式
            storage_path = entry.get("storage_path", "")
            filename = entry.get("original_filename", "?")

        for attempt in range(max_retries):
            try:
                img_bytes = download_image_bytes(storage_path)
                result = ocr_single_from_bytes(img_bytes, filename)

                if img_info:
                    # 新形式: images配列内の該当画像にOCR結果を格納
                    img_info["ocr_done"] = True
                    img_info["confidence"] = result.get("confidence")
                    img_info["is_auto"] = result.get("is_auto", False)
                    img_info["error"] = result.get("error")
                    img_info["store_name"] = result.get("store_name")
                    img_info["store_branch"] = result.get("store_branch")
                    img_info["purchase_date"] = result.get("purchase_date")
                    img_info["items"] = result.get("items", [])
                    img_info["tax_type"] = result.get("tax_type")
                    img_info["tax_adjusted"] = result.get("tax_adjusted", False)
                    img_info["subtotal"] = result.get("subtotal")
                    img_info["total"] = result.get("total")
                    img_info["cp_target_total"] = result.get("cp_target_total", 0)
                    img_info["kao_other_total"] = result.get("kao_other_total", 0)

                    # エントリ全体の集計を更新
                    images = entry["images"]
                    all_done = all(i.get("ocr_done") for i in images)
                    min_conf = min(
                        (i.get("confidence", 0) for i in images if i.get("ocr_done")),
                        default=0,
                    )
                    total_cp = sum(
                        i.get("cp_target_total", 0) for i in images if i.get("ocr_done")
                    )
                    update_data = {
                        "images": images,
                        "confidence": min_conf if all_done else None,
                        "is_auto": min_conf >= CONFIDENCE_THRESHOLD if all_done else False,
                        "cp_target_total": total_cp,
                    }
                    update_entry(campaign, prize, entry_id, update_data)
                else:
                    # 旧形式
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
                    update_entry(campaign, prize, entry_id, update_data)
                return ("ok", file_num, filename, result)
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                try:
                    if img_info:
                        img_info["ocr_done"] = True
                        img_info["error"] = str(e)
                        update_entry(campaign, prize, entry_id, {"images": entry["images"]})
                    else:
                        update_entry(campaign, prize, entry_id, {"error": str(e)})
                except Exception:
                    pass
                return ("error", file_num, filename, str(e))

    # タスク生成: 新形式はimages[]内の各画像ごと、旧形式はエントリごと
    tasks = []
    for entry in targets:
        if _has_images(entry):
            for img in entry["images"]:
                tasks.append((entry, img))
        else:
            tasks.append((entry, None))

    print(f"OCRタスク数: {len(tasks)}件（画像単位）")

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(_ocr_one, entry, img_info): (entry, img_info)
                   for entry, img_info in tasks}
        for future in as_completed(futures):
            completed += 1
            status_type, file_num, filename, data = future.result()
            if status_type == "ok":
                conf = data.get("confidence", 0)
                label = "AUTO" if data.get("is_auto") else "MANUAL"
                print(f"  [{completed}/{len(tasks)}] #{file_num} {filename}: conf={conf:.2f} -> {label}")
                success += 1
            else:
                print(f"  [{completed}/{len(tasks)}] #{file_num} {filename}: ERROR - {data}")
                errors += 1

    # 結果サマリー
    print(f"\n=== OCR再実行完了 ===")
    print(f"処理: {success}件, エラー: {errors}件")

    # 最新の状態を取得
    updated_entries = get_entries(campaign, prize)
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
