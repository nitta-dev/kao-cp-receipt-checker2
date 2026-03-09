"""
花王CP レシートチェックv2 - CLIオーケストレーター
OCR → (アプリで確認・入力) → 抽選 → 納品CSV出力
Firestoreモード + ローカルJSONモード（後方互換）
"""
import argparse
import json
import random
import re
import sys
from pathlib import Path

import pandas as pd

from config import (
    CONFIDENCE_THRESHOLD,
    ADMIN_CHECK_RATIO,
    PRIZES,
    CSV_COLUMNS,
    DEFAULT_CAMPAIGN_ID,
    check_eligibility,
    identify_store_group,
)


DATA_DIR = Path(__file__).parent / "data"
RESULTS_DIR = Path(__file__).parent / "results"


def _extract_filename_number(image_path: str) -> int:
    """画像ファイル名の()内の数字を抽出（ソート用）"""
    name = Path(image_path).name
    m = re.search(r"\((\d+)\)", name)
    if m:
        return int(m.group(1))
    return 9999


def _get_entry_data(entry: dict) -> dict:
    """
    エントリからデータソースを統一的に取得。
    is_auto=True → OCR結果を使用
    is_auto=False & human_input_done=True → human_* フィールドを使用
    """
    if entry.get("is_auto"):
        cp_items = [
            item for item in entry.get("items", [])
            if item.get("is_cp_target")
        ]
        kao_items = [
            item for item in entry.get("items", [])
            if item.get("is_kao") and not item.get("is_cp_target")
        ]
        return {
            "store_name": entry.get("store_name", ""),
            "store_branch": entry.get("store_branch", ""),
            "purchase_date": entry.get("purchase_date", ""),
            "cp_items": cp_items,
            "kao_items": kao_items,
            "unknown_items": [],
            "cp_total": entry.get("cp_target_total", 0),
            "tax_type": entry.get("tax_type", ""),
            "tax_adjusted": entry.get("tax_adjusted", False),
        }
    else:
        return {
            "store_name": entry.get("human_store_name", ""),
            "store_branch": entry.get("human_store_branch", ""),
            "purchase_date": entry.get("human_purchase_date", ""),
            "cp_items": entry.get("human_cp_items", []),
            "kao_items": entry.get("human_kao_items", []),
            "unknown_items": entry.get("human_unknown_items", []),
            "cp_total": entry.get("confirmed_cp_target_total", entry.get("human_cp_total", 0)),
            "tax_type": entry.get("tax_type", ""),
            "tax_adjusted": entry.get("tax_adjusted", False),
        }


def _load_entries(args) -> list[dict]:
    """引数に応じてFirestoreまたはローカルJSONからエントリを読み込む"""
    if getattr(args, "local", False) or not hasattr(args, "campaign"):
        # ローカルJSONモード
        data_path = Path(args.data_json)
        return json.loads(data_path.read_text(encoding="utf-8"))
    else:
        # Firestoreモード
        from firebase_client import init_firebase, get_entries
        init_firebase()
        return get_entries(args.campaign, args.prize)


def _save_entries(entries: list[dict], args, output_path: Path = None):
    """Firestoreまたはローカルに保存"""
    if getattr(args, "local", False) or not hasattr(args, "campaign"):
        path = output_path or Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(entries, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    else:
        from firebase_client import init_firebase, update_entry
        init_firebase()
        for entry in entries:
            entry_id = entry.get("_id")
            if entry_id:
                update_data = {k: v for k, v in entry.items() if k != "_id"}
                update_entry(args.campaign, args.prize, entry_id, update_data)


def cmd_ocr(args):
    """OCR実行: 画像フォルダ → OCR結果"""
    from ocr import ocr_batch, IMAGE_EXTENSIONS

    if getattr(args, "local", False):
        # ローカルモード: 従来通りファイルからOCR
        image_dir = Path(args.image_dir)
        if not image_dir.exists():
            print(f"エラー: 画像フォルダが見つかりません: {image_dir}")
            sys.exit(1)

        image_paths = sorted(
            p for p in image_dir.iterdir()
            if p.suffix.lower() in IMAGE_EXTENSIONS
        )
        print(f"画像数: {len(image_paths)}")

        results = ocr_batch(image_paths, max_workers=args.workers)

        auto_count = sum(1 for r in results if r and r.get("is_auto"))
        manual_count = sum(1 for r in results if r and not r.get("is_auto") and not r.get("error"))
        error_count = sum(1 for r in results if r and r.get("error"))

        print(f"\n=== OCR完了 ===")
        print(f"AI自動確定（信頼度>={CONFIDENCE_THRESHOLD:.0%}）: {auto_count}件")
        print(f"人間手入力（信頼度<{CONFIDENCE_THRESHOLD:.0%}）: {manual_count}件")
        print(f"エラー: {error_count}件")

        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(results, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"結果保存: {output}")
    else:
        # Firestoreモード: Storageから画像DL → OCR → Firestoreに結果登録
        from firebase_client import (
            init_firebase, get_entries, update_entry, download_image_bytes,
        )
        from ocr import ocr_single_from_bytes

        init_firebase()
        entries = get_entries(args.campaign, args.prize)

        # confidence未設定のエントリのみOCR
        targets = [e for e in entries if e.get("confidence") is None and not e.get("error")]
        print(f"OCR対象: {len(targets)}件 / 全{len(entries)}件")

        if not targets:
            print("全てOCR処理済みです。")
            return

        success = 0
        errors = 0
        for i, entry in enumerate(targets, 1):
            entry_id = entry["_id"]
            storage_path = entry.get("storage_path", "")
            filename = entry.get("original_filename", "?")

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
                update_entry(args.campaign, args.prize, entry_id, update_data)
                status = "AUTO" if result.get("is_auto") else "MANUAL"
                print(f"  [{i}/{len(targets)}] {filename}: conf={result.get('confidence', 0):.2f} → {status}")
                success += 1
            except Exception as e:
                update_entry(args.campaign, args.prize, entry_id, {"error": str(e)})
                print(f"  [{i}/{len(targets)}] {filename}: ERROR - {e}")
                errors += 1

        auto_count = sum(
            1 for e in get_entries(args.campaign, args.prize)
            if e.get("is_auto")
        )
        print(f"\n=== OCR完了 ===")
        print(f"処理: {success}件, エラー: {errors}件")
        print(f"AI自動確定: {auto_count}件")


def cmd_split(args):
    """分振: OCR結果JSONを「AI自動」「人間手入力」に分離（後方互換・ローカルのみ）"""
    results_path = Path(args.ocr_json)
    results = json.loads(results_path.read_text(encoding="utf-8"))

    auto_entries = []
    manual_entries = []

    for r in results:
        if r.get("error"):
            manual_entries.append(r)
        elif r.get("is_auto"):
            auto_entries.append(r)
        else:
            manual_entries.append(r)

    check_count = max(1, int(len(manual_entries) * ADMIN_CHECK_RATIO))
    admin_check_indices = set(random.sample(
        range(len(manual_entries)),
        min(check_count, len(manual_entries)),
    ))
    for i, entry in enumerate(manual_entries):
        entry["admin_check_required"] = i in admin_check_indices

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "auto_entries.json").write_text(
        json.dumps(auto_entries, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output_dir / "manual_entries.json").write_text(
        json.dumps(manual_entries, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"AI自動確定: {len(auto_entries)}件 → auto_entries.json")
    print(f"人間手入力: {len(manual_entries)}件 → manual_entries.json")
    print(f"  うち管理者チェック対象: {len(admin_check_indices)}件")


def cmd_lottery(args):
    """抽選: エントリから賞ごとに当選者を抽出"""
    entries = _load_entries(args)
    prize_id = args.prize
    seed = args.seed

    # 未入力チェック
    not_done = []
    for entry in entries:
        if entry.get("is_auto"):
            continue
        if not entry.get("human_input_done"):
            fname = entry.get("original_filename", entry.get("image_path", "?"))
            not_done.append(fname)

    if not_done:
        print(f"⚠️ 警告: {len(not_done)}件の未入力エントリがあります。抽選を中断します。")
        for path in not_done[:10]:
            print(f"  - {Path(path).name}")
        if len(not_done) > 10:
            print(f"  ... 他{len(not_done) - 10}件")
        print("\n全エントリの確認・入力を完了してから再実行してください。")
        sys.exit(1)

    random.seed(seed)

    prize = PRIZES[prize_id]
    eligible = []
    ineligible = []

    for entry in entries:
        if entry.get("unreadable"):
            entry["lottery_result"] = "対象外"
            ineligible.append({**entry, "reason": "レシート読み取り不可"})
            continue

        if entry.get("error") and entry.get("unreadable"):
            entry["lottery_result"] = "対象外"
            ineligible.append({**entry, "reason": "OCRエラー・読み取り不可"})
            continue

        data = _get_entry_data(entry)
        cp_total = data["cp_total"]
        store = data["store_name"]
        ok, reason = check_eligibility(cp_total, store, prize_id)

        if ok:
            eligible.append(entry)
        else:
            ineligible.append({**entry, "reason": reason})

    random.shuffle(eligible)

    winners = eligible[:prize["winners"]]
    reserves = eligible[prize["winners"]:prize["winners"] + prize["reserve"]]
    losers = eligible[prize["winners"] + prize["reserve"]:]

    for entry in winners:
        entry["lottery_result"] = "当選"
    for entry in reserves:
        entry["lottery_result"] = "予備当選"
    for entry in losers:
        entry["lottery_result"] = "落選"
    for entry in ineligible:
        if "lottery_result" not in entry:
            entry["lottery_result"] = "対象外"

    all_results = winners + reserves + losers + ineligible

    # 結果保存
    if getattr(args, "local", False) or not hasattr(args, "campaign"):
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(all_results, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"結果保存: {output}")
    else:
        # Firestoreに結果書き戻し
        from firebase_client import update_entry
        for entry in all_results:
            entry_id = entry.get("_id")
            if entry_id:
                update_entry(
                    args.campaign, args.prize, entry_id,
                    {"lottery_result": entry.get("lottery_result", "")}
                )
        # ローカルにもJSON出力
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        # _id を除去してJSON保存
        clean = [{k: v for k, v in e.items() if k != "_id"} for e in all_results]
        output.write_text(
            json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"Firestore更新完了 + ローカル保存: {output}")

    print(f"\n=== 抽選結果: {prize['name']} ===")
    print(f"応募総数: {len(entries)}")
    print(f"資格あり: {len(eligible)}")
    print(f"当選: {len(winners)} / 予備: {len(reserves)} / 落選: {len(losers)}")
    print(f"対象外: {len(ineligible)}")


def cmd_export(args):
    """納品CSV出力"""
    entries = _load_entries(args)

    filter_mode = args.filter
    if filter_mode == "winners":
        entries = [e for e in entries if e.get("lottery_result") in ("当選", "予備当選")]
    elif filter_mode == "losers":
        entries = [e for e in entries if e.get("lottery_result") == "落選"]
    elif filter_mode == "ineligible":
        entries = [e for e in entries if e.get("lottery_result") == "対象外"]

    # ファイル番号でソート
    entries.sort(key=lambda e: e.get("file_number", _extract_filename_number(
        e.get("original_filename", e.get("image_path", ""))
    )))

    rows = []
    for entry in entries:
        data = _get_entry_data(entry)
        confidence = entry.get("confidence", 0)
        error = entry.get("error")

        if error:
            judgment = "人間入力（エラー）"
            conf_str = "ERROR"
        elif entry.get("is_auto"):
            judgment = "AI自動"
            conf_str = str(confidence)
        else:
            judgment = "人間入力"
            conf_str = str(confidence)

        tax_type = data["tax_type"] if data["tax_type"] else ""
        tax_adjusted = "税補正済" if data["tax_adjusted"] else ""

        filename = entry.get("original_filename", Path(entry.get("image_path", "")).name)
        row = {
            "画像ファイル名": filename,
            "confidence": conf_str,
            "判定": judgment,
            "税区分": tax_type,
            "税補正": tax_adjusted,
            "": "",
            "購入日時": data["purchase_date"],
            "購入チェーン名": data["store_name"],
            "購入店舗": data["store_branch"],
        }

        cp_items = data["cp_items"]
        for i in range(CSV_COLUMNS["cp_target_max"]):
            if i < len(cp_items):
                row[f"CP対象品{_num_label(i+1)}商品名"] = cp_items[i].get("name", "")
                row[f"CP対象品{_num_label(i+1)}金額"] = cp_items[i].get("price", "")
            else:
                row[f"CP対象品{_num_label(i+1)}商品名"] = ""
                row[f"CP対象品{_num_label(i+1)}金額"] = ""

        kao_items = data["kao_items"]
        for i in range(CSV_COLUMNS["kao_other_max"]):
            if i < len(kao_items):
                row[f"その他花王{_num_label(i+1)}商品名"] = kao_items[i].get("name", "")
                row[f"その他花王{_num_label(i+1)}金額"] = kao_items[i].get("price", "")
            else:
                row[f"その他花王{_num_label(i+1)}商品名"] = ""
                row[f"その他花王{_num_label(i+1)}金額"] = ""

        unknown_items = data["unknown_items"]
        for i in range(CSV_COLUMNS["unknown_kao_max"]):
            if i < len(unknown_items):
                row[f"判断つかず{_num_label(i+1)}商品名"] = unknown_items[i].get("name", "")
                row[f"判断つかず{_num_label(i+1)}金額"] = unknown_items[i].get("price", "")
            else:
                row[f"判断つかず{_num_label(i+1)}商品名"] = ""
                row[f"判断つかず{_num_label(i+1)}金額"] = ""

        row["lottery_result"] = entry.get("lottery_result", "")

        rows.append(row)

    df = pd.DataFrame(rows)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    if output.suffix == ".xlsx":
        df.to_excel(output, index=False)
    else:
        df.to_csv(output, index=False, encoding="utf-8-sig")

    print(f"納品データ出力: {output} ({len(rows)}件)")
    if filter_mode != "all":
        print(f"フィルタ: {filter_mode}")


def _num_label(n: int) -> str:
    labels = "①②③④⑤⑥⑦⑧⑨⑩"
    if 1 <= n <= len(labels):
        return labels[n - 1]
    return f"({n})"


def main():
    parser = argparse.ArgumentParser(description="花王CP レシートチェックv2")
    sub = parser.add_subparsers(dest="command")

    # ocr
    p_ocr = sub.add_parser("ocr", help="OCR実行")
    p_ocr.add_argument("image_dir", nargs="?", default=None, help="画像フォルダ（ローカルモード時）")
    p_ocr.add_argument("-o", "--output", default="results/ocr_results.json")
    p_ocr.add_argument("-w", "--workers", type=int, default=3)
    p_ocr.add_argument("--campaign", default=DEFAULT_CAMPAIGN_ID, help="キャンペーンID")
    p_ocr.add_argument("--prize", help="賞ID (S/A/B/C/SP_TSURUHA/SP_WELCIA)")
    p_ocr.add_argument("--local", action="store_true", help="ローカルJSONモード（後方互換）")

    # split (後方互換・ローカルのみ)
    p_split = sub.add_parser("split", help="AI自動/人間手入力に分振（後方互換）")
    p_split.add_argument("ocr_json", help="OCR結果JSON")
    p_split.add_argument("-o", "--output-dir", default="results/")

    # lottery
    p_lottery = sub.add_parser("lottery", help="抽選実行")
    p_lottery.add_argument("data_json", nargs="?", default=None, help="全エントリJSON（ローカルモード時）")
    p_lottery.add_argument("-p", "--prize", required=True, choices=list(PRIZES.keys()))
    p_lottery.add_argument("-s", "--seed", type=int, default=42)
    p_lottery.add_argument("-o", "--output", default="results/lottery_results.json")
    p_lottery.add_argument("--campaign", default=DEFAULT_CAMPAIGN_ID, help="キャンペーンID")
    p_lottery.add_argument("--local", action="store_true", help="ローカルJSONモード（後方互換）")

    # export
    p_export = sub.add_parser("export", help="納品CSV出力")
    p_export.add_argument("data_json", nargs="?", default=None, help="データJSON（ローカルモード時）")
    p_export.add_argument("-o", "--output", default="results/delivery.csv")
    p_export.add_argument(
        "--filter",
        choices=["winners", "losers", "ineligible", "all"],
        default="all",
    )
    p_export.add_argument("-p", "--prize", help="賞ID")
    p_export.add_argument("--campaign", default=DEFAULT_CAMPAIGN_ID, help="キャンペーンID")
    p_export.add_argument("--local", action="store_true", help="ローカルJSONモード（後方互換）")

    args = parser.parse_args()

    if args.command == "ocr":
        if args.local and not args.image_dir:
            print("ローカルモードでは image_dir が必要です。")
            sys.exit(1)
        if not args.local and not args.prize:
            print("Firestoreモードでは --prize が必要です。")
            sys.exit(1)
        cmd_ocr(args)
    elif args.command == "split":
        cmd_split(args)
    elif args.command == "lottery":
        if not args.local and not args.data_json:
            # Firestoreモード: data_json不要
            pass
        elif args.local and not args.data_json:
            print("ローカルモードでは data_json が必要です。")
            sys.exit(1)
        cmd_lottery(args)
    elif args.command == "export":
        if not args.local and not args.data_json and not args.prize:
            print("Firestoreモードでは --prize が必要です。")
            sys.exit(1)
        cmd_export(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
