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
    check_eligibility_group,
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
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        from firebase_client import (
            init_firebase, get_entries, update_entry, download_image_bytes,
        )
        from ocr import ocr_single_from_bytes

        init_firebase()
        entries = get_entries(args.campaign, args.prize)

        # OCR対象の判定: images配列がある場合は各画像単位、旧形式はエントリ単位
        targets = []
        for e in entries:
            if e.get("images"):
                # 新形式: images配列内でOCR未実行の画像があるエントリ
                has_unprocessed = any(
                    not img.get("ocr_done") for img in e["images"]
                )
                if has_unprocessed:
                    targets.append(e)
            else:
                # 旧形式: confidence未設定
                if e.get("confidence") is None and not e.get("error"):
                    targets.append(e)

        # OCRタスクをフラット化（1画像=1タスク）
        ocr_tasks = []
        for e in targets:
            if e.get("images"):
                for img in e["images"]:
                    if not img.get("ocr_done"):
                        ocr_tasks.append((e, img))
            else:
                ocr_tasks.append((e, None))

        print(f"OCR対象: {len(ocr_tasks)}画像 ({len(targets)}エントリ) / 全{len(entries)}件")

        if not ocr_tasks:
            print("全てOCR処理済みです。")
            return

        workers = getattr(args, "workers", 1)
        print(f"並列数: {workers}")

        success = 0
        errors = 0
        lock = threading.Lock()
        completed = 0

        def _ocr_one(entry, img_info=None, max_retries=3):
            """1件のOCR処理（スレッドセーフ・リトライ付き）"""
            import time as _time
            entry_id = entry["_id"]

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
                        update_entry(args.campaign, args.prize, entry_id, update_data)
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
                        update_entry(args.campaign, args.prize, entry_id, update_data)
                    return ("ok", filename, result)
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt  # 1s, 2s, 4s
                        _time.sleep(wait)
                        continue
                    try:
                        if img_info:
                            img_info["ocr_done"] = True
                            img_info["error"] = str(e)
                            update_entry(args.campaign, args.prize, entry_id, {"images": entry["images"]})
                        else:
                            update_entry(args.campaign, args.prize, entry_id, {"error": str(e)})
                    except Exception:
                        pass
                    return ("error", filename, str(e))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_ocr_one, entry, img_info): (entry, img_info)
                for entry, img_info in ocr_tasks
            }
            for future in as_completed(futures):
                completed += 1
                status_type, filename, data = future.result()
                if status_type == "ok":
                    conf = data.get("confidence", 0)
                    label = "AUTO" if data.get("is_auto") else "MANUAL"
                    print(f"  [{completed}/{len(ocr_tasks)}] {filename}: conf={conf:.2f} -> {label}")
                    success += 1
                else:
                    print(f"  [{completed}/{len(ocr_tasks)}] {filename}: ERROR - {data}")
                    errors += 1

        auto_count = sum(
            1 for e in get_entries(args.campaign, args.prize)
            if e.get("is_auto")
        )
        print(f"\n=== OCR完了 ===")
        print(f"処理: {success}画像, エラー: {errors}画像")
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


def _get_file_number(entry: dict) -> int:
    """エントリからfile_numberを取得（後方互換）"""
    if "file_number" in entry:
        return entry["file_number"]
    return _extract_filename_number(
        entry.get("original_filename", entry.get("image_path", ""))
    )


def _get_entry_key(entry: dict) -> str:
    """エントリの一意キーを取得（form_id_answer_id or file_number）"""
    if "form_id" in entry and "answer_id" in entry:
        return f"{entry['form_id']}_{entry['answer_id']}"
    return str(_get_file_number(entry))


def _get_entry_data_multi(entry: dict) -> dict:
    """
    新形式エントリ（images配列）からデータを統一的に取得。
    各画像のOCR結果を合算する。
    人間入力済みの場合はhuman_*フィールドを優先。
    """
    if entry.get("human_input_done"):
        return _get_entry_data(entry)

    images = entry.get("images", [])
    if not images:
        return _get_entry_data(entry)

    # images配列内の各画像のOCR結果を合算
    cp_total = 0
    store_names = []
    store_branches = []
    all_cp_items = []
    all_kao_items = []

    for img in images:
        if img.get("is_auto") or img.get("ocr_done"):
            cp_items = [i for i in img.get("items", []) if i.get("is_cp_target")]
            kao_items = [i for i in img.get("items", []) if i.get("is_kao") and not i.get("is_cp_target")]
            all_cp_items.extend(cp_items)
            all_kao_items.extend(kao_items)
            cp_total += img.get("cp_target_total", 0)
            if img.get("store_name"):
                store_names.append(img["store_name"])
            if img.get("store_branch"):
                store_branches.append(img["store_branch"])

    return {
        "store_name": store_names[0] if store_names else "",
        "store_branch": store_branches[0] if store_branches else "",
        "purchase_date": "",
        "cp_items": all_cp_items,
        "kao_items": all_kao_items,
        "unknown_items": [],
        "cp_total": cp_total,
        "tax_type": "",
        "tax_adjusted": False,
    }


def cmd_lottery(args):
    """抽選: エントリから賞ごとに当選者を抽出（新形式: 1エントリ=1応募者）"""
    entries = _load_entries(args)
    prize_id = args.prize
    seed = args.seed

    # 未入力チェック
    not_done = []
    for entry in entries:
        if entry.get("is_auto"):
            continue
        if not entry.get("human_input_done"):
            key = _get_entry_key(entry)
            not_done.append(key)

    if not_done:
        print(f"⚠️ 警告: {len(not_done)}件の未入力エントリがあります。抽選を中断します。")
        for key in not_done[:10]:
            print(f"  - {key}")
        if len(not_done) > 10:
            print(f"  ... 他{len(not_done) - 10}件")
        print("\n全エントリの確認・入力を完了してから再実行してください。")
        sys.exit(1)

    random.seed(seed)

    prize = PRIZES[prize_id]

    eligible_entries = []
    ineligible_entries = []

    for entry in entries:
        # 読み取り不可チェック
        if entry.get("unreadable"):
            entry["lottery_result"] = "対象外"
            ineligible_entries.append({**entry, "reason": "レシート読み取り不可"})
            continue

        # 新形式（images配列）の場合は合算データを使用
        if entry.get("images"):
            data = _get_entry_data_multi(entry)
        else:
            data = _get_entry_data(entry)

        cp_total = data["cp_total"]
        store = data["store_name"]
        store_groups_list = [identify_store_group(store)]

        # 複数レシートの場合、各レシートの店舗も考慮
        if entry.get("images"):
            store_groups_list = []
            for img in entry["images"]:
                s = img.get("store_name", "")
                store_groups_list.append(identify_store_group(s))

        entry["aggregated_cp_total"] = cp_total

        ok, reason = check_eligibility_group(cp_total, store_groups_list, prize_id)

        if ok:
            eligible_entries.append(entry)
        else:
            ineligible_entries.append({**entry, "reason": reason})

    # --- 抽選 ---
    random.shuffle(eligible_entries)

    winners = eligible_entries[:prize["winners"]]
    reserves = eligible_entries[prize["winners"]:prize["winners"] + prize["reserve"]]
    losers = eligible_entries[prize["winners"] + prize["reserve"]:]

    for e in winners:
        e["lottery_result"] = "当選"
    for e in reserves:
        e["lottery_result"] = "予備当選"
    for e in losers:
        e["lottery_result"] = "落選"
    for e in ineligible_entries:
        if "lottery_result" not in e:
            e["lottery_result"] = "対象外"

    all_results = winners + reserves + losers + ineligible_entries

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
                    {
                        "lottery_result": entry.get("lottery_result", ""),
                        "aggregated_cp_total": entry.get("aggregated_cp_total"),
                    }
                )
        # ローカルにもJSON出力
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        clean = [{k: v for k, v in e.items() if k != "_id"} for e in all_results]
        output.write_text(
            json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"Firestore更新完了 + ローカル保存: {output}")

    print(f"\n=== 抽選結果: {prize['name']} ===")
    print(f"応募総数: {len(entries)}件")
    print(f"資格あり: {len(eligible_entries)}人")
    print(f"当選: {len(winners)} / 予備: {len(reserves)} / 落選: {len(losers)}")
    print(f"対象外: {len(ineligible_entries)}件")


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

    # ソート: form_id → answer_id or file_number
    entries.sort(key=lambda e: (
        e.get("form_id", 0),
        e.get("answer_id", _extract_filename_number(
            e.get("original_filename", e.get("image_path", ""))
        )),
    ))

    rows = []
    for entry in entries:
        entry_key = _get_entry_key(entry)
        images = entry.get("images", [])

        # 共通の応募者情報
        base_row = {
            "エントリID": entry_key,
            "フォームID": entry.get("form_id", ""),
            "回答ID": entry.get("answer_id", ""),
            "姓": entry.get("last_name", ""),
            "名": entry.get("first_name", ""),
            "郵便番号": entry.get("postal_code", ""),
            "都道府県": entry.get("prefecture", ""),
            "市区町村": entry.get("city", ""),
            "番地": entry.get("address1", ""),
            "番地2": entry.get("address2", ""),
            "ビル名": entry.get("building", ""),
            "電話番号": entry.get("phone", ""),
            "メール": entry.get("email", ""),
            "年齢": entry.get("age", ""),
            "性別": entry.get("gender", ""),
            "レシート枚数": entry.get("receipt_count", 1),
            "lottery_result": entry.get("lottery_result", ""),
        }

        # 新形式: 各画像を別行に展開
        ocr_images = [img for img in images if img.get("ocr_done")]
        if ocr_images and not entry.get("human_input_done"):
            for img in ocr_images:
                row = dict(base_row)
                row["レシート番号"] = img.get("receipt_number", "")
                row["confidence"] = img.get("confidence", "")
                row["判定"] = "AI自動" if img.get("is_auto") else "人間入力"
                row["税区分"] = img.get("tax_type", "")
                row["税補正"] = "税補正済" if img.get("tax_adjusted") else ""
                row["購入チェーン名"] = img.get("store_name", "")
                row["購入店舗"] = img.get("store_branch", "")

                cp_items = [i for i in img.get("items", []) if i.get("is_cp_target")]
                for i in range(CSV_COLUMNS["cp_target_max"]):
                    if i < len(cp_items):
                        row[f"CP対象品{_num_label(i+1)}商品名"] = cp_items[i].get("name", "")
                        row[f"CP対象品{_num_label(i+1)}金額"] = cp_items[i].get("price", "")
                    else:
                        row[f"CP対象品{_num_label(i+1)}商品名"] = ""
                        row[f"CP対象品{_num_label(i+1)}金額"] = ""

                row["CP合計"] = img.get("cp_target_total", 0)

                kao_items = [i for i in img.get("items", []) if i.get("is_kao") and not i.get("is_cp_target")]
                for i in range(CSV_COLUMNS["kao_other_max"]):
                    if i < len(kao_items):
                        row[f"その他花王{_num_label(i+1)}商品名"] = kao_items[i].get("name", "")
                        row[f"その他花王{_num_label(i+1)}金額"] = kao_items[i].get("price", "")
                    else:
                        row[f"その他花王{_num_label(i+1)}商品名"] = ""
                        row[f"その他花王{_num_label(i+1)}金額"] = ""

                unknown_items = []
                for i in range(CSV_COLUMNS["unknown_kao_max"]):
                    row[f"判断つかず{_num_label(i+1)}商品名"] = ""
                    row[f"判断つかず{_num_label(i+1)}金額"] = ""

                rows.append(row)
        else:
            # 旧形式 or 人間入力済み: 1エントリ=1行
            if entry.get("images") and not entry.get("human_input_done"):
                data = _get_entry_data_multi(entry)
            else:
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

            row = dict(base_row)
            row["レシート番号"] = ""
            row["confidence"] = conf_str
            row["判定"] = judgment
            row["税区分"] = data["tax_type"] if data["tax_type"] else ""
            row["税補正"] = "税補正済" if data["tax_adjusted"] else ""
            row["購入チェーン名"] = data["store_name"]
            row["購入店舗"] = data["store_branch"]

            cp_items = data["cp_items"]
            for i in range(CSV_COLUMNS["cp_target_max"]):
                if i < len(cp_items):
                    row[f"CP対象品{_num_label(i+1)}商品名"] = cp_items[i].get("name", "")
                    row[f"CP対象品{_num_label(i+1)}金額"] = cp_items[i].get("price", "")
                else:
                    row[f"CP対象品{_num_label(i+1)}商品名"] = ""
                    row[f"CP対象品{_num_label(i+1)}金額"] = ""

            row["CP合計"] = data["cp_total"]

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
