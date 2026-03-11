"""
花王CP レシートチェックv2 - 既存JSONデータのFirestore移行
ワンショットスクリプト: ocr_all_171.json → Firestore
"""
import argparse
import json
import re
import sys
from pathlib import Path

# scriptsフォルダから実行するためのパス追加
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from firebase_client import init_firebase, create_entries_batch, upload_image

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}


def extract_file_number(filename: str) -> int:
    """ファイル名の()内の数字を抽出"""
    m = re.search(r"\((\d+)\)", filename)
    if m:
        return int(m.group(1))
    return 9999


def convert_entry(entry: dict, campaign: str, prize: str) -> dict:
    """
    旧フラット構造 → Firestore構造に変換。

    旧: store_name, items, confidence, human_store_name, human_cp_items 等がフラット
    新: ocr_result / human_input にネスト + メタ情報
    """
    image_path = entry.get("image_path", "")
    filename = Path(image_path).name
    file_number = extract_file_number(filename)

    # OCR結果をネスト
    ocr_result = {
        "store_name": entry.get("store_name"),
        "store_branch": entry.get("store_branch"),
        "purchase_date": entry.get("purchase_date"),
        "items": entry.get("items", []),
        "tax_type": entry.get("tax_type"),
        "tax_adjusted": entry.get("tax_adjusted", False),
        "subtotal": entry.get("subtotal"),
        "total": entry.get("total"),
        "cp_target_total": entry.get("cp_target_total", 0),
        "kao_other_total": entry.get("kao_other_total", 0),
    }

    # 人間入力結果をネスト（あれば）
    human_input = None
    if entry.get("human_input_done"):
        human_input = {
            "store_name": entry.get("human_store_name", ""),
            "store_branch": entry.get("human_store_branch", ""),
            "purchase_date": entry.get("human_purchase_date", ""),
            "cp_items": entry.get("human_cp_items", []),
            "kao_items": entry.get("human_kao_items", []),
            "unknown_items": entry.get("human_unknown_items", []),
            "cp_total": entry.get("human_cp_total", 0),
        }

    # 確認済みデータ（confirmedフィールド）
    confirmed = None
    if entry.get("confirmed_store_name") is not None:
        confirmed = {
            "store_name": entry.get("confirmed_store_name", ""),
            "store_branch": entry.get("confirmed_store_branch", ""),
            "purchase_date": entry.get("confirmed_purchase_date", ""),
            "cp_target_total": entry.get("confirmed_cp_target_total", 0),
            "items": entry.get("confirmed_items", []),
        }

    new_entry = {
        "file_number": file_number,
        "original_filename": filename,
        "storage_path": f"{campaign}/{prize}/{filename}",
        "confidence": entry.get("confidence"),
        "is_auto": entry.get("is_auto", False),
        "human_input_done": entry.get("human_input_done", False),
        "unreadable": entry.get("unreadable", False),
        "error": entry.get("error"),
        "lottery_result": entry.get("lottery_result"),
        "assigned_to": None,
        "assigned_at": None,
        "ocr_result": ocr_result,
        "human_input": human_input,
        "confirmed": confirmed,
        # 旧フラットフィールドも互換用に保持
        "image_path": image_path,
        "store_name": entry.get("store_name"),
        "store_branch": entry.get("store_branch"),
        "purchase_date": entry.get("purchase_date"),
        "items": entry.get("items", []),
        "tax_type": entry.get("tax_type"),
        "tax_adjusted": entry.get("tax_adjusted", False),
        "cp_target_total": entry.get("cp_target_total", 0),
        "human_store_name": entry.get("human_store_name"),
        "human_store_branch": entry.get("human_store_branch"),
        "human_purchase_date": entry.get("human_purchase_date"),
        "human_cp_items": entry.get("human_cp_items"),
        "human_kao_items": entry.get("human_kao_items"),
        "human_unknown_items": entry.get("human_unknown_items"),
        "human_cp_total": entry.get("human_cp_total", 0),
        "confirmed_store_name": entry.get("confirmed_store_name"),
        "confirmed_store_branch": entry.get("confirmed_store_branch"),
        "confirmed_purchase_date": entry.get("confirmed_purchase_date"),
        "confirmed_cp_target_total": entry.get("confirmed_cp_target_total"),
        "confirmed_items": entry.get("confirmed_items"),
    }

    return new_entry


def main():
    parser = argparse.ArgumentParser(
        description="既存JSONデータをFirestoreに移行"
    )
    parser.add_argument(
        "json_file",
        nargs="?",
        default=None,
        help="移行元JSONファイル（省略時: results/ocr_all_171.json）",
    )
    parser.add_argument("--campaign", default="kao_cp_2026", help="キャンペーンID")
    parser.add_argument("--prize", default="B", help="賞ID")
    parser.add_argument(
        "--upload-images",
        action="store_true",
        help="画像もStorageにアップロードする",
    )
    parser.add_argument("--dry-run", action="store_true", help="変換結果を表示のみ")
    args = parser.parse_args()

    # JSONファイル特定
    if args.json_file:
        json_path = Path(args.json_file)
    else:
        json_path = Path(__file__).parent / "results" / "ocr_all_171.json"

    if not json_path.exists():
        print(f"エラー: ファイルが見つかりません: {json_path}")
        sys.exit(1)

    entries = json.loads(json_path.read_text(encoding="utf-8"))
    print(f"読み込み: {json_path} ({len(entries)}件)")

    # 変換
    new_entries = []
    for entry in entries:
        new_entry = convert_entry(entry, args.campaign, args.prize)
        new_entries.append(new_entry)

    # 統計
    auto_count = sum(1 for e in new_entries if e.get("is_auto"))
    done_count = sum(1 for e in new_entries if e.get("human_input_done"))
    error_count = sum(1 for e in new_entries if e.get("error"))

    print(f"\n変換結果:")
    print(f"  合計: {len(new_entries)}件")
    print(f"  AI自動確定: {auto_count}件")
    print(f"  人間入力済: {done_count}件")
    print(f"  エラー: {error_count}件")

    if args.dry_run:
        print("\n[DRY RUN] 最初の3件のプレビュー:")
        for e in new_entries[:3]:
            print(f"  #{e['file_number']:04d} {e['original_filename']}")
            print(f"    confidence={e.get('confidence')}, is_auto={e.get('is_auto')}")
            print(f"    human_input_done={e.get('human_input_done')}")
        return

    # Firebase初期化
    init_firebase()

    # 画像アップロード（オプション）
    if args.upload_images:
        print("\n画像アップロード中...")
        uploaded = 0
        for entry in new_entries:
            image_path = entry.get("image_path", "")
            if image_path and Path(image_path).exists():
                try:
                    upload_image(image_path, entry["storage_path"])
                    uploaded += 1
                    if uploaded % 10 == 0:
                        print(f"  {uploaded}/{len(new_entries)} アップロード済み")
                except Exception as e:
                    print(f"  ❌ {entry['original_filename']}: {e}")
        print(f"画像アップロード完了: {uploaded}件")

    # Firestoreに登録
    print(f"\nFirestoreにエントリ登録中...")
    create_entries_batch(args.campaign, args.prize, new_entries)

    print(f"\n=== 移行完了 ===")
    print(f"Firestore: campaigns/{args.campaign}/prizes/{args.prize}/entries/ ({len(new_entries)}件)")


if __name__ == "__main__":
    main()
