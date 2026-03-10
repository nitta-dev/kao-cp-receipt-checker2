"""
花王CP レシートチェックv2 - CSV+画像インポート
Google Forms応募データ（CSV）と画像フォルダを読み込み、
Firestoreにエントリ登録 + Storageに画像アップロード。

Usage:
    python import_csv.py "data/A賞" --campaign kao_cp_2026 --prize A
    python import_csv.py "data/A賞" --campaign kao_cp_2026 --prize A --dry-run
    python import_csv.py "data/A賞" --campaign kao_cp_2026 --prize A --skip-upload
"""
import argparse
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from config import CSV_COLUMNS_MAP, DEFAULT_CAMPAIGN_ID
from firebase_client import (
    init_firebase,
    upload_image,
    create_entries_batch,
)


def _convert_pdf_to_image(pdf_path: Path) -> Path:
    """PDFの1ページ目をJPG画像に変換し、一時ファイルパスを返す"""
    import fitz  # PyMuPDF
    import tempfile
    import os

    doc = fitz.open(str(pdf_path))
    page = doc[0]
    # 高解像度でレンダリング（2倍）
    mat = fitz.Matrix(2, 2)
    pix = page.get_pixmap(matrix=mat)
    fd, tmp_path = tempfile.mkstemp(suffix=".jpg")
    os.close(fd)
    pix.save(tmp_path)
    doc.close()
    return Path(tmp_path)

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".bmp", ".pdf"}


def find_csv(data_dir: Path) -> Path:
    """data_dir内のCSVを自動検出（1ファイル前提）"""
    csvs = list(data_dir.glob("*.csv"))
    if not csvs:
        # 親ディレクトリも探す
        csvs = list(data_dir.parent.glob("*.csv"))
    if not csvs:
        print(f"エラー: CSVファイルが見つかりません: {data_dir}")
        sys.exit(1)
    if len(csvs) > 1:
        print(f"CSVが複数あります。最初のファイルを使用: {csvs[0].name}")
    return csvs[0]


def parse_csv(csv_path: Path) -> pd.DataFrame:
    """CSVを読み込み、列名をマッピング"""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    # 逆引きマッピング（日本語列名 → 英語キー）
    reverse_map = {v: k for k, v in CSV_COLUMNS_MAP.items()}

    rename = {}
    for col in df.columns:
        if col in reverse_map:
            rename[col] = reverse_map[col]
    df = df.rename(columns=rename)

    # form_id, answer_id を int 化
    df["form_id"] = pd.to_numeric(df["form_id"], errors="coerce").astype("Int64")
    df["answer_id"] = pd.to_numeric(df["answer_id"], errors="coerce").astype("Int64")
    df["age"] = pd.to_numeric(df["age"], errors="coerce").astype("Int64")

    return df


def scan_image_folders(data_dir: Path) -> dict[int, dict[int, dict[int, Path]]]:
    """
    画像フォルダを走査して {form_id: {answer_id: {receipt_n: path}}} の辞書を構築。

    フォルダ構造:
        A賞①_21554/21554_1枚目/(10)image.jpg
        A賞⑦_21540_1~2枚目/21540_1枚目/(10)image.jpg
    """
    images: dict[int, dict[int, dict[int, Path]]] = {}

    # data_dir直下のフォルダを走査
    for prize_folder in sorted(data_dir.iterdir()):
        if not prize_folder.is_dir():
            continue

        # フォルダ名からform_idを抽出（末尾の連続数字 or _数字_ パターン）
        folder_name = prize_folder.name
        # パターン: A賞①_21554 or A賞⑦_21540_1~2枚目
        form_id_match = re.search(r"_(\d{4,6})(?:_|$)", folder_name)
        if not form_id_match:
            continue
        form_id = int(form_id_match.group(1))

        # このフォルダ直下の n枚目 サブフォルダを走査
        for sub in sorted(prize_folder.iterdir()):
            if not sub.is_dir():
                continue
            # パターン: 21554_1枚目
            n_match = re.search(r"_(\d+)枚目", sub.name)
            if not n_match:
                continue
            receipt_n = int(n_match.group(1))

            # 画像ファイルを走査
            for img_path in sorted(sub.iterdir()):
                if img_path.suffix.lower() not in IMAGE_EXTENSIONS:
                    continue
                # ファイル名から answer_id を抽出: (10)image.jpg → 10
                aid_match = re.match(r"\((\d+)\)", img_path.name)
                if not aid_match:
                    continue
                answer_id = int(aid_match.group(1))

                if form_id not in images:
                    images[form_id] = {}
                if answer_id not in images[form_id]:
                    images[form_id][answer_id] = {}
                images[form_id][answer_id][receipt_n] = img_path

    return images


def build_entries(
    df: pd.DataFrame,
    images: dict[int, dict[int, dict[int, Path]]],
    campaign: str,
    prize: str,
) -> list[dict]:
    """CSVと画像辞書からエントリリストを構築"""
    entries = []
    matched = 0
    no_images = 0

    for _, row in df.iterrows():
        form_id = int(row["form_id"]) if pd.notna(row["form_id"]) else None
        answer_id = int(row["answer_id"]) if pd.notna(row["answer_id"]) else None

        if form_id is None or answer_id is None:
            continue

        entry_id = f"{form_id}_{answer_id}"

        # 画像マッチング
        entry_images = []
        form_images = images.get(form_id, {})
        answer_images = form_images.get(answer_id, {})

        for receipt_n in sorted(answer_images.keys()):
            img_path = answer_images[receipt_n]
            storage_path = f"{campaign}/{prize}/{form_id}_{answer_id}_{receipt_n}.jpg"
            entry_images.append({
                "receipt_number": receipt_n,
                "original_filename": img_path.name,
                "local_path": str(img_path),
                "storage_path": storage_path,
            })

        if entry_images:
            matched += 1
        else:
            no_images += 1

        # 応募者情報
        def _str(val):
            if pd.isna(val):
                return ""
            return str(val).strip()

        def _int(val):
            if pd.isna(val):
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        # 応募者共通情報
        applicant_info = {
            "form_id": form_id,
            "answer_id": answer_id,
            "answered_at": _str(row.get("answered_at")),
            "last_name": _str(row.get("last_name")),
            "first_name": _str(row.get("first_name")),
            "postal_code": _str(row.get("postal_code")),
            "prefecture": _str(row.get("prefecture")),
            "city": _str(row.get("city")),
            "address1": _str(row.get("address1")),
            "address2": _str(row.get("address2")),
            "building": _str(row.get("building")),
            "phone": _str(row.get("phone")),
            "email": _str(row.get("email")),
            "age": _int(row.get("age")),
            "gender": _str(row.get("gender")),
            "q2_course": _str(row.get("q2_course")),
        }

        # レシート1枚ごとに1エントリを作成
        if not entry_images:
            # 画像なし: 従来通り1エントリ
            entry = {
                **applicant_info,
                "_id": entry_id,
                "group_id": entry_id,
                "receipt_number": 1,
                "group_receipt_count": 0,
                "images": [],
                "receipt_count": 0,
                "is_auto": False,
                "human_input_done": False,
                "confidence": None,
                "error": None,
                "assigned_to": None,
                "assigned_at": None,
            }
            entries.append(entry)
        else:
            for img in entry_images:
                r_num = img["receipt_number"]
                receipt_entry_id = f"{entry_id}_r{r_num}" if len(entry_images) > 1 else entry_id
                entry = {
                    **applicant_info,
                    "_id": receipt_entry_id,
                    "group_id": entry_id,
                    "receipt_number": r_num,
                    "group_receipt_count": len(entry_images),
                    "images": [img],
                    "receipt_count": 1,
                    "storage_path": img["storage_path"],
                    "original_filename": img["original_filename"],
                    "is_auto": False,
                    "human_input_done": False,
                    "confidence": None,
                    "error": None,
                    "assigned_to": None,
                    "assigned_at": None,
                }
                entries.append(entry)

    print(f"CSV行数: {len(df)}")
    print(f"画像あり: {matched}件, 画像なし: {no_images}件")
    return entries


def upload_entry_images(
    entries: list[dict],
    workers: int = 5,
) -> tuple[int, int]:
    """全エントリの画像をStorageにアップロード"""
    # アップロード対象をフラット化
    tasks = []
    for entry in entries:
        for img in entry.get("images", []):
            local_path = img.get("local_path")
            if local_path:
                tasks.append((Path(local_path), img["storage_path"]))

    if not tasks:
        print("アップロード対象の画像がありません。")
        return 0, 0

    print(f"\n画像アップロード開始: {len(tasks)}枚 (workers={workers})")

    success = 0
    failed = 0

    def _upload_one(local: Path, storage: str):
        """1枚アップロード（PDF変換対応）"""
        converted = None
        try:
            if local.suffix.lower() == ".pdf":
                converted = _convert_pdf_to_image(local)
                upload_image(converted, storage)
            else:
                upload_image(local, storage)
        finally:
            if converted:
                try:
                    converted.unlink(missing_ok=True)
                except Exception:
                    pass

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_upload_one, local, storage): (local, storage)
            for local, storage in tasks
        }
        for i, future in enumerate(as_completed(futures), 1):
            local, storage = futures[future]
            try:
                future.result()
                success += 1
                if i % 50 == 0 or i == len(tasks):
                    print(f"  [{i}/{len(tasks)}] アップロード中...")
            except Exception as e:
                failed += 1
                print(f"  NG {local.name}: {e}")

    print(f"アップロード完了: 成功={success}, 失敗={failed}")
    return success, failed


def register_entries(
    entries: list[dict],
    campaign: str,
    prize: str,
):
    """エントリをFirestoreに一括登録（local_pathは除去）"""
    clean_entries = []
    for entry in entries:
        e = dict(entry)
        # images内のlocal_pathを除去
        if "images" in e:
            e["images"] = [
                {k: v for k, v in img.items() if k != "local_path"}
                for img in e["images"]
            ]
        clean_entries.append(e)

    print(f"\nFirestoreにエントリ登録中... ({len(clean_entries)}件)")
    create_entries_batch(campaign, prize, clean_entries)
    print("Firestore登録完了")


def main():
    parser = argparse.ArgumentParser(
        description="CSV+画像インポート → Firebase Storage + Firestore"
    )
    parser.add_argument("data_dir", help="データフォルダパス（CSV+画像フォルダ）")
    parser.add_argument("--campaign", default=DEFAULT_CAMPAIGN_ID, help="キャンペーンID")
    parser.add_argument("--prize", required=True, help="賞ID (S/A/B/C/SP_TSURUHA/SP_WELCIA)")
    parser.add_argument("--workers", type=int, default=5, help="並列アップロード数")
    parser.add_argument("--dry-run", action="store_true", help="マッチング確認のみ（アップロード・登録しない）")
    parser.add_argument("--skip-upload", action="store_true", help="画像アップロードをスキップ（Firestoreのみ登録）")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"エラー: フォルダが見つかりません: {data_dir}")
        sys.exit(1)

    # 1. CSV読み込み
    csv_path = find_csv(data_dir)
    print(f"CSV: {csv_path.name}")
    df = parse_csv(csv_path)
    print(f"応募数: {len(df)}件")

    # 2. 画像フォルダ走査
    print(f"\n画像フォルダ走査中...")
    images = scan_image_folders(data_dir)

    # 統計表示
    total_images = 0
    for fid, answers in images.items():
        for aid, receipts in answers.items():
            total_images += len(receipts)
    form_ids = sorted(images.keys())
    print(f"フォームID: {form_ids}")
    print(f"画像総数: {total_images}枚")
    for fid in form_ids:
        answer_count = len(images[fid])
        img_count = sum(len(r) for r in images[fid].values())
        print(f"  form_id={fid}: {answer_count}応募, {img_count}枚")

    # 3. エントリ構築
    print(f"\nエントリ構築中...")
    entries = build_entries(df, images, args.campaign, args.prize)
    print(f"エントリ数: {len(entries)}件")

    # 画像枚数分布
    receipt_dist = {}
    for e in entries:
        n = e["receipt_count"]
        receipt_dist[n] = receipt_dist.get(n, 0) + 1
    print(f"レシート枚数分布: {dict(sorted(receipt_dist.items()))}")

    if args.dry_run:
        print(f"\n[DRY RUN] インポートは実行しません。")
        # 画像なしのエントリを表示
        no_img = [e for e in entries if e["receipt_count"] == 0]
        if no_img:
            print(f"\n[WARNING] 画像なしの応募: {len(no_img)}件")
            for e in no_img[:10]:
                print(f"  {e['_id']} ({e['last_name']} {e['first_name']})")
            if len(no_img) > 10:
                print(f"  ... 他{len(no_img) - 10}件")
        return

    # 4. Firebase初期化
    init_firebase()

    # 5. 画像アップロード
    if not args.skip_upload:
        upload_entry_images(entries, workers=args.workers)
    else:
        print("\n--skip-upload: 画像アップロードをスキップ")

    # 6. Firestore登録
    register_entries(entries, args.campaign, args.prize)

    print(f"\n=== インポート完了 ===")
    print(f"エントリ数: {len(entries)}件")
    print(f"Firestore: campaigns/{args.campaign}/prizes/{args.prize}/entries/")
    print(f"Storage: {args.campaign}/{args.prize}/")


if __name__ == "__main__":
    main()
