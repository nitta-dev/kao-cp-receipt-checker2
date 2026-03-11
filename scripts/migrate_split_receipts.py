"""
マイグレーション: 複数画像エントリをレシート単位に分割

既存の 1エントリ=1応募者(複数images) → 1エントリ=1レシート(group_idで紐づけ)

Usage:
    python migrate_split_receipts.py --campaign kao_cp_2026 --prize A --dry-run
    python migrate_split_receipts.py --campaign kao_cp_2026 --prize A
"""
import argparse
import os
import sys
from pathlib import Path

# Windows cp932 対策
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if sys.stdout.encoding != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# scriptsフォルダから実行するためのパス追加
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import DEFAULT_CAMPAIGN_ID, PRIZES
from firebase_client import init_firebase, get_entries, update_entry, _entries_ref, _db


def migrate(campaign: str, prize_id: str, dry_run: bool = True):
    init_firebase()
    entries = get_entries(campaign, prize_id)

    print(f"対象: campaigns/{campaign}/prizes/{prize_id}")
    print(f"全エントリ数: {len(entries)}")
    print(f"モード: {'DRY RUN（変更なし）' if dry_run else '本番実行'}")
    print()

    already_migrated = 0
    single_updated = 0
    multi_split = 0
    new_entries_count = 0

    ref = _entries_ref(campaign, prize_id)
    db_client = _db()

    for entry in entries:
        entry_id = entry["_id"]
        images = entry.get("images", [])
        receipt_count = entry.get("receipt_count", len(images))

        # 既にマイグレーション済み（group_idがある）
        if entry.get("group_id"):
            already_migrated += 1
            continue

        if receipt_count <= 1:
            # 単一レシート: group_idとreceipt_numberを追加するだけ
            single_updated += 1
            print(f"  [単一] {entry_id} → group_id={entry_id}, receipt_number=1")
            if not dry_run:
                update_entry(campaign, prize_id, entry_id, {
                    "group_id": entry_id,
                    "receipt_number": 1,
                    "group_receipt_count": max(receipt_count, 1),
                })
        else:
            # 複数レシート: 分割
            multi_split += 1
            print(f"  [分割] {entry_id} → {receipt_count}エントリに分割")

            # 応募者共通フィールド
            applicant_fields = [
                "form_id", "answer_id", "answered_at",
                "last_name", "first_name",
                "postal_code", "prefecture", "city",
                "address1", "address2", "building",
                "phone", "email", "age", "gender", "q2_course",
            ]
            applicant_info = {k: entry.get(k) for k in applicant_fields if k in entry}

            new_entries = []
            for img in images:
                r_num = img.get("receipt_number", 1)
                new_id = f"{entry_id}_r{r_num}"

                new_entry = {
                    **applicant_info,
                    "_id": new_id,
                    "group_id": entry_id,
                    "receipt_number": r_num,
                    "group_receipt_count": receipt_count,
                    "images": [img],
                    "receipt_count": 1,
                    "storage_path": img.get("storage_path"),
                    "original_filename": img.get("original_filename"),
                    # OCR結果（画像単位）
                    "confidence": img.get("confidence"),
                    "is_auto": img.get("is_auto", False),
                    "error": img.get("error"),
                    "store_name": img.get("store_name"),
                    "store_branch": img.get("store_branch"),
                    "purchase_date": img.get("purchase_date"),
                    "items": img.get("items", []),
                    "tax_type": img.get("tax_type"),
                    "tax_adjusted": img.get("tax_adjusted", False),
                    "subtotal": img.get("subtotal"),
                    "total": img.get("total"),
                    "cp_target_total": img.get("cp_target_total", 0),
                    "kao_other_total": img.get("kao_other_total", 0),
                    # ステータスリセット（レシート単位で再入力が必要）
                    "human_input_done": False,
                    "assigned_to": None,
                    "assigned_at": None,
                }

                # 元エントリが人間入力済みの場合、r1にhuman_*を引き継ぐ
                if entry.get("human_input_done") and r_num == images[0].get("receipt_number", 1):
                    for k, v in entry.items():
                        if k.startswith("human_") or k in ("unreadable", "confirmed_store_name",
                            "confirmed_store_branch", "confirmed_purchase_date",
                            "confirmed_cp_target_total", "completed_by"):
                            new_entry[k] = v
                    new_entry["human_input_done"] = True

                new_entries.append(new_entry)
                print(f"    → {new_id} (レシート{r_num})")

            new_entries_count += len(new_entries)

            if not dry_run:
                # バッチ書き込み: 新エントリ作成 + 旧エントリ削除
                batch = db_client.batch()
                for ne in new_entries:
                    doc_id = ne.pop("_id")
                    from datetime import datetime, timezone, timedelta
                    JST = timezone(timedelta(hours=9))
                    now = datetime.now(JST)
                    ne["created_at"] = now
                    ne["updated_at"] = now
                    batch.set(ref.document(doc_id), ne)
                # 旧エントリ削除
                batch.delete(ref.document(entry_id))
                batch.commit()
                print(f"    ✅ {entry_id} → {len(new_entries)}件作成、元エントリ削除")

    print()
    print("=== サマリー ===")
    print(f"マイグレーション済み（スキップ）: {already_migrated}")
    print(f"単一レシート（group_id追加のみ）: {single_updated}")
    print(f"複数レシート（分割）: {multi_split} → 新規{new_entries_count}エントリ")
    if dry_run:
        print("\n⚠️ DRY RUNモードのため変更は行われていません。")
        print("本番実行するには --dry-run を外してください。")


def main():
    parser = argparse.ArgumentParser(description="複数画像エントリをレシート単位に分割")
    parser.add_argument("--campaign", default=DEFAULT_CAMPAIGN_ID, help="キャンペーンID")
    parser.add_argument("--prize", required=True, help="賞ID", choices=list(PRIZES.keys()))
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="変更を行わずに結果だけ表示")
    args = parser.parse_args()

    migrate(args.campaign, args.prize, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
