"""
花王CP レシートチェックv2 - Firebase クライアント
Firestore / Storage の一元管理モジュール
"""
import io
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.cloud.firestore_v1 import FieldFilter
from PIL import Image
try:
    from pillow_heif import register_heif_opener
    register_heif_opener()
except ImportError:
    pass

from config import LOCK_TIMEOUT_MINUTES

JST = timezone(timedelta(hours=9))

# === Firebase 初期化 ===

_app = None


def init_firebase():
    """
    Firebase初期化。以下の優先順で認証情報を取得:
    1. Streamlit secrets (st.secrets["firebase"])
    2. 環境変数 FIREBASE_CREDENTIALS (JSONファイルパス)
    3. 環境変数 FIREBASE_CREDENTIALS_JSON (JSON文字列)
    """
    global _app
    if _app is not None:
        return

    # 既に他で初期化済みの場合
    try:
        _app = firebase_admin.get_app()
        return
    except ValueError:
        pass  # まだ初期化されていない

    cred = None
    bucket_name = None

    # 1) Streamlit secrets
    try:
        import streamlit as st
        if "firebase" in st.secrets:
            secret = dict(st.secrets["firebase"])
            bucket_name = secret.pop("storage_bucket", None)
            cred = credentials.Certificate(secret)
    except Exception:
        pass

    # 2) 環境変数: ファイルパス
    if cred is None:
        cred_path = os.environ.get("FIREBASE_CREDENTIALS")
        if cred_path and Path(cred_path).exists():
            cred = credentials.Certificate(cred_path)

    # 3) 環境変数: JSON文字列
    if cred is None:
        cred_json = os.environ.get("FIREBASE_CREDENTIALS_JSON")
        if cred_json:
            cred = credentials.Certificate(json.loads(cred_json))

    if cred is None:
        raise RuntimeError(
            "Firebase認証情報が見つかりません。"
            "FIREBASE_CREDENTIALS（ファイルパス）または "
            "FIREBASE_CREDENTIALS_JSON（JSON文字列）を設定してください。"
        )

    if bucket_name is None:
        bucket_name = os.environ.get(
            "FIREBASE_STORAGE_BUCKET",
            "kao-cp-receipt-checker.firebasestorage.app",
        )

    _app = firebase_admin.initialize_app(cred, {"storageBucket": bucket_name})


def _db():
    init_firebase()
    return firestore.client()


def _bucket():
    init_firebase()
    return storage.bucket()


# === コレクションパス ===

def _entries_ref(campaign: str, prize: str):
    """entries コレクション参照"""
    return _db().collection("campaigns").document(campaign) \
        .collection("prizes").document(prize) \
        .collection("entries")


# === エントリ CRUD ===

def get_entries(campaign: str, prize: str, order_by: str | None = None) -> list[dict]:
    """エントリ一覧を取得（ソートキー自動判定）"""
    ref = _entries_ref(campaign, prize)
    docs = list(ref.stream())
    entries = []
    for doc in docs:
        entry = doc.to_dict()
        entry["_id"] = doc.id
        entries.append(entry)

    # ソート: form_idがあればform_id→answer_id順、なければfile_number順
    if entries and "form_id" in entries[0]:
        entries.sort(key=lambda e: (e.get("form_id", 0), e.get("answer_id", 0)))
    elif entries and "file_number" in entries[0]:
        entries.sort(key=lambda e: e.get("file_number", 0))
    return entries


def get_entry(campaign: str, prize: str, entry_id: str) -> dict | None:
    """1件取得"""
    doc = _entries_ref(campaign, prize).document(entry_id).get()
    if doc.exists:
        entry = doc.to_dict()
        entry["_id"] = doc.id
        return entry
    return None


def update_entry(campaign: str, prize: str, entry_id: str, data: dict):
    """1件更新（マージ）"""
    data["updated_at"] = _now()
    _entries_ref(campaign, prize).document(entry_id).update(data)


def create_entries_batch(campaign: str, prize: str, entries: list[dict]):
    """一括登録（500件ずつバッチ書き込み）"""
    db = _db()
    ref = _entries_ref(campaign, prize)

    batch_size = 500
    for i in range(0, len(entries), batch_size):
        chunk = entries[i:i + batch_size]
        batch = db.batch()
        for entry in chunk:
            doc_ref = ref.document(entry.get("_id") or None)
            entry["created_at"] = _now()
            entry["updated_at"] = _now()
            entry.pop("_id", None)
            batch.set(doc_ref, entry)
        batch.commit()
        print(f"  バッチ書き込み: {i + 1}〜{min(i + len(chunk), len(entries))} / {len(entries)}")


def delete_all_entries(campaign: str, prize: str) -> int:
    """指定賞のエントリを全削除（バッチ）"""
    db = _db()
    ref = _entries_ref(campaign, prize)
    docs = list(ref.stream())
    count = len(docs)
    batch_size = 500
    for i in range(0, count, batch_size):
        chunk = docs[i:i + batch_size]
        batch = db.batch()
        for doc in chunk:
            batch.delete(doc.reference)
        batch.commit()
    return count


def delete_storage_folder(prefix: str) -> int:
    """Storageの指定プレフィックス配下を全削除"""
    bucket = _bucket()
    blobs = list(bucket.list_blobs(prefix=prefix))
    count = len(blobs)
    for blob in blobs:
        blob.delete()
    return count


# === 進捗統計 ===

def get_prize_stats(campaign: str, prize: str) -> dict:
    """1賞の進捗統計"""
    entries = get_entries(campaign, prize)
    total = len(entries)
    auto = sum(1 for e in entries if e.get("is_auto") and not e.get("human_input_done"))
    human_done = sum(1 for e in entries if e.get("human_input_done"))
    needs_input = sum(
        1 for e in entries
        if not e.get("is_auto") and not e.get("human_input_done")
    )
    error = sum(1 for e in entries if e.get("error"))
    done = auto + human_done
    progress = done / total if total > 0 else 0

    return {
        "total": total,
        "auto": auto,
        "human_done": human_done,
        "needs_input": needs_input,
        "error": error,
        "done": done,
        "progress": progress,
    }


def get_all_prize_stats(campaign: str) -> dict[str, dict]:
    """全賞の統計（ダッシュボード用）"""
    from config import PRIZES
    stats = {}
    for prize_id in PRIZES:
        stats[prize_id] = get_prize_stats(campaign, prize_id)
    return stats


# === 排他制御（ロック） ===

def claim_entry(campaign: str, prize: str, entry_id: str, user: str) -> bool:
    """
    エントリをロック（トランザクション）。
    成功=True、他の人がロック中=False。
    """
    db = _db()
    doc_ref = _entries_ref(campaign, prize).document(entry_id)

    @firestore.transactional
    def _claim(transaction):
        doc = doc_ref.get(transaction=transaction)
        if not doc.exists:
            return False

        data = doc.to_dict()
        assigned_to = data.get("assigned_to")
        assigned_at = data.get("assigned_at")

        # ロック済み？
        if assigned_to and assigned_to != user:
            # タイムアウトチェック
            if assigned_at and _is_lock_valid(assigned_at):
                return False  # 他の人がロック中
            # タイムアウト → ロック奪取

        transaction.update(doc_ref, {
            "assigned_to": user,
            "assigned_at": _now(),
        })
        return True

    transaction = db.transaction()
    return _claim(transaction)


def release_entry(campaign: str, prize: str, entry_id: str):
    """ロック解放"""
    _entries_ref(campaign, prize).document(entry_id).update({
        "assigned_to": None,
        "assigned_at": None,
    })


def get_next_unclaimed_entry(
    campaign: str, prize: str, user: str
) -> dict | None:
    """
    次の未入力・未ロックエントリを取得してロック。
    すでに自分がロック中のエントリがあればそれを返す。
    """
    entries = get_entries(campaign, prize)
    now = _now()

    # まず自分がロック中のエントリを探す
    for entry in entries:
        if entry.get("assigned_to") == user and not entry.get("human_input_done"):
            if not entry.get("is_auto"):
                return entry

    # 未入力・未ロックのエントリを探す
    for entry in entries:
        if entry.get("is_auto") or entry.get("human_input_done"):
            continue
        assigned_to = entry.get("assigned_to")
        if assigned_to and assigned_to != user:
            assigned_at = entry.get("assigned_at")
            if assigned_at and _is_lock_valid(assigned_at):
                continue  # 他の人がロック中
        # ロック取得を試みる
        if claim_entry(campaign, prize, entry["_id"], user):
            entry["assigned_to"] = user
            return entry

    return None  # 全エントリ処理済み


def get_entry_display_key(entry: dict) -> str:
    """エントリの表示用キーを取得（form_id_answer_id or file_number）"""
    if "form_id" in entry and "answer_id" in entry:
        return f"{entry['form_id']}_{entry['answer_id']}"
    return str(entry.get("file_number", "?"))


def get_active_workers(campaign: str, prize: str) -> list[dict]:
    """現在作業中のユーザー一覧"""
    entries = get_entries(campaign, prize)
    workers = []
    now = datetime.now(JST)
    for entry in entries:
        assigned_to = entry.get("assigned_to")
        assigned_at = entry.get("assigned_at")
        if assigned_to and assigned_at and _is_lock_valid(assigned_at):
            elapsed = now - _to_datetime(assigned_at)
            workers.append({
                "user": assigned_to,
                "entry_id": entry["_id"],
                "display_key": get_entry_display_key(entry),
                "minutes_ago": int(elapsed.total_seconds() / 60),
            })
    return workers


# === チームメンバー管理 ===

def _members_ref(campaign: str):
    """メンバーコレクション参照"""
    return _db().collection("campaigns").document(campaign).collection("members")


def get_team_members(campaign: str) -> list[str]:
    """チームメンバー一覧を取得（名前順）"""
    docs = _members_ref(campaign).order_by("name").stream()
    members = [doc.to_dict().get("name", "") for doc in docs]
    return members


def add_team_member(campaign: str, name: str):
    """メンバー追加"""
    name = name.strip()
    if not name:
        return
    # 重複チェック
    existing = get_team_members(campaign)
    if name in existing:
        return
    _members_ref(campaign).add({"name": name, "created_at": _now()})


def remove_team_member(campaign: str, name: str):
    """メンバー削除"""
    docs = _members_ref(campaign).where("name", "==", name).stream()
    for doc in docs:
        doc.reference.delete()


def init_team_members(campaign: str, default_members: list[str]):
    """メンバーが未登録なら初期メンバーを登録"""
    existing = get_team_members(campaign)
    if existing:
        return existing
    for name in default_members:
        add_team_member(campaign, name)
    return default_members


# === 担当者別統計 ===

def get_worker_stats(campaign: str, prize: str) -> dict[str, int]:
    """賞ごとの担当者別完了件数"""
    entries = get_entries(campaign, prize)
    stats = {}
    for e in entries:
        completed_by = e.get("completed_by")
        if completed_by and e.get("human_input_done"):
            stats[completed_by] = stats.get(completed_by, 0) + 1
    return stats


def get_all_worker_stats(campaign: str) -> dict[str, dict[str, int]]:
    """全賞横断の担当者別統計 {member: {prize: count}}"""
    from config import PRIZES
    result = {}
    for prize_id in PRIZES:
        prize_stats = get_worker_stats(campaign, prize_id)
        for member, count in prize_stats.items():
            if member not in result:
                result[member] = {}
            result[member][prize_id] = count
    return result


# === Firebase Storage (画像) ===

def upload_image(
    local_path: str | Path,
    storage_path: str,
    max_size: int = 1600,
    quality: int = 85,
) -> str:
    """
    画像を圧縮してStorageにアップロード。
    Returns: Storage上のパス
    """
    local_path = Path(local_path)
    img = Image.open(local_path)

    # EXIF回転
    from PIL import ImageOps
    img = ImageOps.exif_transpose(img)

    # リサイズ（長辺max_size）
    w, h = img.size
    if max(w, h) > max_size:
        ratio = max_size / max(w, h)
        img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)

    # RGB変換（RGBA→RGB）
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    # JPEG圧縮してアップロード
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=quality, optimize=True)
    buf.seek(0)

    blob = _bucket().blob(storage_path)
    blob.upload_from_file(buf, content_type="image/jpeg")

    return storage_path


def get_image_url(storage_path: str, expiration_minutes: int = 60) -> str:
    """署名付きURL取得（デフォルト1時間有効）"""
    blob = _bucket().blob(storage_path)
    url = blob.generate_signed_url(
        expiration=timedelta(minutes=expiration_minutes),
        method="GET",
    )
    return url


def download_image_bytes(storage_path: str) -> bytes:
    """Storage画像をバイト列でダウンロード"""
    blob = _bucket().blob(storage_path)
    return blob.download_as_bytes()


# === ユーティリティ ===

def _now():
    return datetime.now(JST)


def _to_datetime(val) -> datetime:
    """Firestore Timestamp or datetime を datetime に変換"""
    if hasattr(val, "timestamp"):
        # Firestore Timestamp
        return datetime.fromtimestamp(val.timestamp(), tz=JST)
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=JST)
        return val
    return datetime.now(JST)


def _is_lock_valid(assigned_at) -> bool:
    """ロックがまだ有効（タイムアウトしていない）か"""
    dt = _to_datetime(assigned_at)
    return datetime.now(JST) - dt < timedelta(minutes=LOCK_TIMEOUT_MINUTES)
