"""
花王CP レシートチェックv2 - 画像一括アップロードCLI
画像フォルダを圧縮してFirebase Storageにアップ + Firestoreエントリ自動生成
"""
import argparse
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from firebase_client import init_firebase, upload_image, create_entries_batch

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}


def extract_file_number(filename: str) -> int:
    """ファイル名から番号を抽出: (123).jpg → 123"""
    m = re.search(r"\((\d+)\)", filename)
    if m:
        return int(m.group(1))
    # 番号なし → ファイル名のハッシュベースで一意な番号
    return hash(filename) % 100000


def upload_single(
    local_path: Path, campaign: str, prize: str
) -> dict | None:
    """1枚の画像をアップロードし、エントリデータを返す"""
    file_number = extract_file_number(local_path.name)
    storage_path = f"{campaign}/{prize}/{local_path.name}"

    try:
        upload_image(local_path, storage_path)
        return {
            "file_number": file_number,
            "original_filename": local_path.name,
            "storage_path": storage_path,
            "is_auto": False,
            "human_input_done": False,
            "confidence": None,
            "error": None,
            "assigned_to": None,
            "assigned_at": None,
        }
    except Exception as e:
        print(f"  NG {local_path.name}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="画像一括アップロード → Firebase Storage + Firestore"
    )
    parser.add_argument("image_dir", help="画像フォルダパス")
    parser.add_argument("--campaign", required=True, help="キャンペーンID")
    parser.add_argument("--prize", required=True, help="賞ID (S/A/B/C/SP_TSURUHA/SP_WELCIA)")
    parser.add_argument("--workers", type=int, default=5, help="並列アップロード数")
    parser.add_argument("--dry-run", action="store_true", help="アップロードせず一覧表示のみ")
    parser.add_argument("--images-only", action="store_true", help="画像のみアップロード（Firestoreエントリ作成をスキップ）")
    args = parser.parse_args()

    image_dir = Path(args.image_dir)
    if not image_dir.exists():
        print(f"エラー: フォルダが見つかりません: {image_dir}")
        sys.exit(1)

    image_paths = sorted(
        p for p in image_dir.iterdir()
        if p.suffix.lower() in IMAGE_EXTENSIONS
    )
    print(f"画像数: {len(image_paths)}")

    if not image_paths:
        print("アップロード対象の画像がありません。")
        sys.exit(0)

    if args.dry_run:
        print("\n[DRY RUN] 以下の画像がアップロード対象:")
        for p in image_paths:
            num = extract_file_number(p.name)
            print(f"  #{num:04d} {p.name} ({p.stat().st_size / 1024:.0f}KB)")
        print(f"\n合計: {len(image_paths)}枚")
        return

    # Firebase初期化
    init_firebase()

    # 並列アップロード
    entries = []
    failed = 0
    print(f"\nアップロード開始 (workers={args.workers})...")

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(upload_single, p, args.campaign, args.prize): p
            for p in image_paths
        }
        for i, future in enumerate(as_completed(futures), 1):
            path = futures[future]
            result = future.result()
            if result:
                entries.append(result)
                print(f"  OK [{i}/{len(image_paths)}] {path.name}")
            else:
                failed += 1

    # file_number順にソート
    entries.sort(key=lambda e: e["file_number"])

    # Firestoreに一括登録（--images-onlyの場合はスキップ）
    if args.images_only:
        print(f"\n--images-only: Firestoreエントリ登録をスキップ")
    else:
        print(f"\nFirestoreにエントリ登録中... ({len(entries)}件)")
        create_entries_batch(args.campaign, args.prize, entries)

    print(f"\n=== 完了 ===")
    print(f"成功: {len(entries)}件")
    print(f"失敗: {failed}件")
    print(f"Storage: {args.campaign}/{args.prize}/")
    print(f"Firestore: campaigns/{args.campaign}/prizes/{args.prize}/entries/")


if __name__ == "__main__":
    main()
