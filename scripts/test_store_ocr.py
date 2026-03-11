"""店舗名OCR改善テスト: Entry 1（マツキヨ法典駅前）と21536_300（ドンキ誤読み）を再OCR"""
import os
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

# scriptsフォルダから実行するためのパス追加
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

os.environ.setdefault(
    "FIREBASE_CREDENTIALS",
    os.path.join(str(Path(__file__).resolve().parent.parent), ".secrets", "serviceAccountKey.json"),
)

from firebase_client import init_firebase, get_entries, download_image_bytes
from ocr import ocr_single_from_bytes

init_firebase()
print("Firebase OK", flush=True)

entries = get_entries("kao_cp_2026", "A")
print(f"Total entries: {len(entries)}", flush=True)

# Entry 1 (index 0) と form_id=21536, answer_id=300
targets = []
targets.append(("Entry[1]", entries[0]))
for e in entries:
    if e.get("form_id") == 21536 and e.get("answer_id") == 300:
        targets.append(("21536_300", e))
        break

for label, entry in targets:
    fid = entry.get("form_id")
    aid = entry.get("answer_id")
    print(f"\n=== {label} (form_id={fid}, answer_id={aid}) ===", flush=True)

    images = entry.get("images", [])
    for i, img in enumerate(images):
        if not isinstance(img, dict):
            continue
        old_store = img.get("store_name", "?")
        old_branch = img.get("store_branch", "?")
        storage_path = img.get("storage_path", "")
        filename = img.get("original_filename", f"{fid}_{aid}_{i+1}.jpg")

        print(f"  [image {i+1}] OLD: store={old_store}, branch={old_branch}", flush=True)

        try:
            img_bytes = download_image_bytes(storage_path)
            result = ocr_single_from_bytes(img_bytes, filename)
            print(f"  [image {i+1}] NEW: store={result.get('store_name')}, branch={result.get('store_branch')}", flush=True)
            print(f"  [image {i+1}] confidence={result.get('confidence', 0):.2f}", flush=True)
        except Exception as e:
            print(f"  [image {i+1}] ERROR: {e}", flush=True)

print("\nDone.", flush=True)
