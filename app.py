"""
花王CP レシートチェックv2 - Streamlit UI
Firebase Firestore/Storage対応 + マルチユーザー排他制御
"""
import io
from datetime import date, time, datetime
from pathlib import Path

import streamlit as st
from PIL import Image, ImageOps

from config import (
    CONFIDENCE_THRESHOLD,
    STORE_GROUPS,
    PRIZES,
    TEAM_MEMBERS,
    DEFAULT_CAMPAIGN_ID,
    check_eligibility,
)
from firebase_client import (
    init_firebase,
    get_entries,
    update_entry,
    get_prize_stats,
    get_all_prize_stats,
    claim_entry,
    release_entry,
    get_next_unclaimed_entry,
    get_active_workers,
    get_entry_display_key,
    get_image_url,
    download_image_bytes,
    get_team_members,
    add_team_member,
    remove_team_member,
    init_team_members,
    get_all_worker_stats,
)

# ==== ページ設定 ====
st.set_page_config(page_title="花王CP レシートチェックv2", layout="wide")


# === 画像表示ヘルパー ===

def _image_to_base64(img: Image.Image) -> str:
    """PIL ImageをBase64文字列に変換"""
    import base64
    buf = io.BytesIO()
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    img.save(buf, format="JPEG", quality=90)
    return base64.b64encode(buf.getvalue()).decode()


def _render_zoomable_image(img: Image.Image, zoom: int, container_key: str):
    """ズーム可能な画像を枠内スクロールで表示"""
    b64 = _image_to_base64(img)
    width_pct = zoom
    container_id = f"img-container-{container_key}"
    html = f"""
    <div id="{container_id}" style="
        width: 100%;
        height: 600px;
        overflow: auto;
        border: 1px solid #ddd;
        border-radius: 4px;
        cursor: grab;
    ">
        <img src="data:image/jpeg;base64,{b64}"
             style="width: {width_pct}%; max-width: none;"
             draggable="false" />
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def render_receipt_image_from_url(storage_path: str, rotation: int = 0, zoom: int = 100):
    """Storage画像をURL経由で表示（回転・ズーム対応）"""
    if not storage_path:
        st.warning("画像パスが設定されていません")
        return

    try:
        if rotation or zoom != 100:
            img_bytes = download_image_bytes(storage_path)
            img = Image.open(io.BytesIO(img_bytes))
            img = ImageOps.exif_transpose(img)
            if rotation:
                img = img.rotate(-rotation, expand=True)
            _render_zoomable_image(img, zoom, storage_path)
        else:
            try:
                url = _get_cached_image_url(storage_path)
                st.image(url, use_container_width=True)
            except Exception:
                img_bytes = download_image_bytes(storage_path)
                st.image(img_bytes, use_container_width=True)
    except Exception as e:
        st.error(f"画像読み込みエラー: {e}")


@st.cache_data(ttl=3000)  # 50分キャッシュ（URLは60分有効）
def _get_cached_image_url(storage_path: str) -> str:
    return get_image_url(storage_path)


def render_receipt_image_local(image_path: str, rotation: int = 0):
    """ローカル画像表示（後方互換）"""
    path = Path(image_path)
    if not path.exists():
        st.warning(f"画像が見つかりません: {image_path}")
        return
    img = Image.open(path)
    img = ImageOps.exif_transpose(img)
    if rotation:
        img = img.rotate(-rotation, expand=True)
    st.image(img, use_container_width=True)


def render_receipt_image(entry: dict, rotation: int = 0, zoom: int = 100):
    """エントリに応じて適切な画像表示方法を選択（複数画像対応）"""
    images = entry.get("images")

    if images and len(images) > 0:
        # 新形式: images配列
        if len(images) == 1:
            render_receipt_image_from_url(images[0]["storage_path"], rotation, zoom)
        else:
            # 複数枚: タブ表示（画像タブは同時実行でも問題ない）
            tab_labels = [f"レシート{img['receipt_number']}枚目" for img in images]
            tabs = st.tabs(tab_labels)
            for tab, img in zip(tabs, images):
                with tab:
                    render_receipt_image_from_url(img["storage_path"], rotation, zoom)
        return

    # 旧形式: storage_path / image_path
    storage_path = entry.get("storage_path")
    image_path = entry.get("image_path", "")

    if storage_path:
        render_receipt_image_from_url(storage_path, rotation, zoom)
    elif image_path and Path(image_path).exists():
        render_receipt_image_local(image_path, rotation)
    else:
        st.warning(f"画像が見つかりません: {image_path or storage_path}")


def get_eligibility_display(cp_total: int) -> str:
    """CP合計額からどの賞に資格があるか表示文字列を生成"""
    results = []
    for pid in ["S", "A", "B", "C"]:
        prize = PRIZES[pid]
        if cp_total >= prize["min_amount"]:
            results.append(f"{pid}賞")
    if results:
        return f"資格あり({', '.join(results)})"
    return "資格なし"


def extract_filename_number(entry: dict) -> int | str:
    """エントリからファイル番号または表示キーを取得"""
    if "form_id" in entry and "answer_id" in entry:
        return entry["answer_id"]
    if "file_number" in entry:
        return entry["file_number"]
    name = Path(entry.get("image_path", "")).name
    if "(" in name and ")" in name:
        try:
            return int(name.split("(")[1].split(")")[0])
        except (ValueError, IndexError):
            pass
    return 9999


def get_entry_label(entry: dict) -> str:
    """エントリの表示ラベルを取得"""
    return get_entry_display_key(entry)


# ============================================================
def _safe_int(val, default=0) -> int:
    """Firestore値を安全にintに変換（文字列・None対応）"""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# 共通: 入力フォーム描画 & 保存処理
# ============================================================
def _render_entry_form(entry: dict, entry_id: str, key_prefix: str = "") -> dict:
    """共通入力フォームを描画し、現在の入力値を返す"""
    p = key_prefix

    # 店舗情報
    store_chain = st.text_input(
        "🏪 店舗チェーン名",
        value=entry.get("human_store_name", ""),
        placeholder="例: ツルハドラッグ",
        key=f"{p}store_chain_{entry_id}",
    )
    store_branch = st.text_input(
        "🏪 店舗名（支店）",
        value=entry.get("human_store_branch", ""),
        placeholder="例: 勝田店",
        key=f"{p}store_branch_{entry_id}",
    )
    # 購入日時: カレンダー + 時刻ピッカー
    existing_date_val = None
    existing_time_val = time(0, 0)
    existing_str = entry.get("human_purchase_date", "")
    if existing_str:
        try:
            dt = datetime.strptime(existing_str.strip(), "%Y-%m-%d %H:%M")
            existing_date_val = dt.date()
            existing_time_val = dt.time()
        except ValueError:
            pass

    col_date, col_hour, col_min = st.columns([3, 1, 1])
    with col_date:
        purchase_date_val = st.date_input(
            "📅 購入日",
            value=existing_date_val,
            key=f"{p}purchase_date_{entry_id}",
        )
    with col_hour:
        purchase_hour = st.selectbox(
            "🕐 時",
            options=list(range(24)),
            index=existing_time_val.hour,
            key=f"{p}purchase_hour_{entry_id}",
        )
    with col_min:
        purchase_min = st.selectbox(
            "分",
            options=list(range(60)),
            index=existing_time_val.minute,
            key=f"{p}purchase_min_{entry_id}",
        )

    if purchase_date_val:
        purchase_date = f"{purchase_date_val.strftime('%Y-%m-%d')} {purchase_hour:02d}:{purchase_min:02d}"
    else:
        purchase_date = ""

    st.divider()

    # === CP対象品 ===
    st.markdown("#### 🎯 CP対象品")
    cp_count_key = f"{p}cp_count_{entry_id}"
    if cp_count_key not in st.session_state:
        existing_cp = entry.get("human_cp_items", [])
        st.session_state[cp_count_key] = max(len(existing_cp) if existing_cp else 0, 1)

    cp_items = []
    for i in range(st.session_state[cp_count_key]):
        existing = (entry.get("human_cp_items") or [{}])[i] if i < len(entry.get("human_cp_items") or []) else {}
        c1, c2 = st.columns([3, 1])
        with c1:
            name = st.text_input(
                f"CP{i+1} 商品名",
                value=existing.get("name", ""),
                key=f"{p}cp_name_{entry_id}_{i}",
                placeholder="商品名",
            )
        with c2:
            price = st.number_input(
                f"CP{i+1} 金額",
                min_value=0,
                value=existing.get("price", 0),
                step=1,
                key=f"{p}cp_price_{entry_id}_{i}",
            )
        if name:
            cp_items.append({"name": name, "price": price})

    if st.button("+ CP対象行を追加", key=f"{p}add_cp_{entry_id}"):
        st.session_state[cp_count_key] += 1
        st.rerun()

    cp_total = sum(item["price"] for item in cp_items)
    st.metric("CP対象合計", f"¥{cp_total:,}")

    st.divider()

    # === 花王その他 ===
    st.markdown("#### 🔵 その他花王製品")
    kao_count_key = f"{p}kao_count_{entry_id}"
    if kao_count_key not in st.session_state:
        existing_kao = entry.get("human_kao_items", [])
        st.session_state[kao_count_key] = max(len(existing_kao) if existing_kao else 0, 1)

    kao_items = []
    for i in range(st.session_state[kao_count_key]):
        existing = (entry.get("human_kao_items") or [{}])[i] if i < len(entry.get("human_kao_items") or []) else {}
        c1, c2 = st.columns([3, 1])
        with c1:
            name = st.text_input(
                f"花王{i+1} 商品名",
                value=existing.get("name", ""),
                key=f"{p}kao_name_{entry_id}_{i}",
                placeholder="商品名",
            )
        with c2:
            price = st.number_input(
                f"花王{i+1} 金額",
                min_value=0,
                value=existing.get("price", 0),
                step=1,
                key=f"{p}kao_price_{entry_id}_{i}",
            )
        if name:
            kao_items.append({"name": name, "price": price})

    if st.button("+ 花王その他行を追加", key=f"{p}add_kao_{entry_id}"):
        st.session_state[kao_count_key] += 1
        st.rerun()

    st.divider()

    # === 判断つかず ===
    st.markdown("#### ❓ 判断つかず")
    unk_count_key = f"{p}unk_count_{entry_id}"
    if unk_count_key not in st.session_state:
        existing_unk = entry.get("human_unknown_items", [])
        st.session_state[unk_count_key] = max(len(existing_unk) if existing_unk else 0, 1)

    unknown_items = []
    for i in range(st.session_state[unk_count_key]):
        existing = (entry.get("human_unknown_items") or [{}])[i] if i < len(entry.get("human_unknown_items") or []) else {}
        c1, c2 = st.columns([3, 1])
        with c1:
            name = st.text_input(
                f"不明{i+1} 商品名",
                value=existing.get("name", ""),
                key=f"{p}unk_name_{entry_id}_{i}",
                placeholder="フリーテキスト",
            )
        with c2:
            price = st.number_input(
                f"不明{i+1} 金額",
                min_value=0,
                value=existing.get("price", 0),
                step=1,
                key=f"{p}unk_price_{entry_id}_{i}",
            )
        if name:
            unknown_items.append({"name": name, "price": price})

    if st.button("+ 判断つかず行を追加", key=f"{p}add_unk_{entry_id}"):
        st.session_state[unk_count_key] += 1
        st.rerun()

    st.divider()

    # レシート読み取り不可
    unreadable = st.checkbox(
        "📛 レシートが読み取れない",
        value=entry.get("unreadable", False),
        key=f"{p}unreadable_{entry_id}",
    )

    return {
        "store_chain": store_chain,
        "store_branch": store_branch,
        "purchase_date": purchase_date,
        "cp_items": cp_items,
        "kao_items": kao_items,
        "unknown_items": unknown_items,
        "cp_total": cp_total,
        "unreadable": unreadable,
    }


def _get_validation_warnings(form_data: dict) -> list[str]:
    """入力データのバリデーション"""
    if form_data["unreadable"]:
        return []

    warnings = []
    missing = []
    if not (form_data["store_chain"] or "").strip():
        missing.append("店舗名")
    if not (form_data["purchase_date"] or "").strip():
        missing.append("日付")
    if missing:
        warnings.append(f"⚠️ {' と '.join(missing)}が入っていないですが、あっていますか？")

    if not form_data["cp_items"]:
        warnings.append("⚠️ CP対象商品欄が空ですが、あっていますか？")

    return warnings


def _save_entry_to_firestore(
    campaign: str, prize: str, entry_id: str, form_data: dict,
    user: str = "",
):
    """フォームデータをFirestoreに保存"""
    data = {
        "human_input_done": True,
        "is_auto": False,
        "human_store_name": form_data["store_chain"],
        "human_store_branch": form_data["store_branch"],
        "human_purchase_date": form_data["purchase_date"],
        "human_cp_items": form_data["cp_items"],
        "human_kao_items": form_data["kao_items"],
        "human_unknown_items": form_data["unknown_items"],
        "human_cp_total": form_data["cp_total"],
        "unreadable": form_data["unreadable"],
        "confirmed_store_name": form_data["store_chain"],
        "confirmed_store_branch": form_data["store_branch"],
        "confirmed_purchase_date": form_data["purchase_date"],
        "confirmed_cp_target_total": form_data["cp_total"],
        "confirmed_items": form_data["cp_items"] + form_data["kao_items"],
        "human_input": {
            "store_name": form_data["store_chain"],
            "store_branch": form_data["store_branch"],
            "purchase_date": form_data["purchase_date"],
            "cp_items": form_data["cp_items"],
            "kao_items": form_data["kao_items"],
            "unknown_items": form_data["unknown_items"],
            "cp_total": form_data["cp_total"],
        },
        "completed_by": user,
        # ロック解放
        "assigned_to": None,
        "assigned_at": None,
    }
    update_entry(campaign, prize, entry_id, data)


def _cleanup_edit_state(entry_id: str, edit_key: str):
    """編集モード終了時にsession_stateをクリア"""
    st.session_state[edit_key] = False
    for k in list(st.session_state.keys()):
        if k.startswith("edit_") and k.endswith(f"_{entry_id}"):
            del st.session_state[k]
        elif k.startswith("edit_") and f"_{entry_id}_" in k:
            del st.session_state[k]


# ============================================================
# メイン画面
# ============================================================
def main():
    st.sidebar.title("花王CP レシートチェックv2")

    # Firebase初期化
    try:
        init_firebase()
    except Exception as e:
        st.error(f"Firebase接続エラー: {e}")
        st.info("FIREBASE_CREDENTIALS 環境変数を設定してください。")
        st.stop()

    # キャンペーン
    campaign = st.sidebar.text_input(
        "🏷️ キャンペーンID",
        value=DEFAULT_CAMPAIGN_ID,
        key="campaign_id",
    )

    # チームメンバーをFirestoreから取得（初回は config.py のデフォルトで初期化）
    members = init_team_members(campaign, TEAM_MEMBERS)

    # 担当者選択（session_stateで確実に保持）
    if "selected_user" not in st.session_state:
        st.session_state["selected_user"] = members[0] if members else ""

    # selectboxでkeyを使わず、戻り値で制御（keyだとrerun時にリセットされる問題の回避）
    saved_user = st.session_state["selected_user"]
    default_idx = members.index(saved_user) if saved_user in members else 0

    user = st.sidebar.selectbox(
        "👤 担当者",
        members,
        index=default_idx,
        key="current_user_select",
    )

    # 担当者が変わったらエントリを切り替え
    if user != st.session_state["selected_user"]:
        st.session_state["selected_user"] = user
        st.session_state.pop("current_entry", None)
        st.cache_data.clear()
        st.rerun()

    # メンバー管理
    with st.sidebar.expander("👥 メンバー管理"):
        # 追加
        new_member = st.text_input("新しいメンバー名", key="new_member_input")
        if st.button("追加", key="add_member_btn") and new_member.strip():
            add_team_member(campaign, new_member.strip())
            st.rerun()

        # 削除
        if members:
            del_member = st.selectbox("削除するメンバー", members, key="del_member_select")
            if st.button("削除", key="del_member_btn"):
                remove_team_member(campaign, del_member)
                st.rerun()

    # 賞選択
    prize_id = st.sidebar.selectbox(
        "🏆 対象賞",
        list(PRIZES.keys()),
        index=list(PRIZES.keys()).index("A"),
        format_func=lambda pid: f"{pid}: {PRIZES[pid]['name']}",
        key="prize_select",
    )
    prize = PRIZES[prize_id]
    st.sidebar.caption(f"最低金額: ¥{prize['min_amount']:,}")

    # モード切替
    mode = st.sidebar.radio(
        "モード",
        ["📝 レシート入力", "📊 ダッシュボード"],
        key="mode_select",
    )

    # 店舗グループ参照
    with st.sidebar.expander("📋 店舗グループ一覧"):
        for group_id, stores in STORE_GROUPS.items():
            group_label = "ツルハグループ" if group_id == "tsuruha" else "ウエルシアグループ"
            st.markdown(f"**{group_label}**")
            st.markdown(", ".join(stores[:5]) + "...")

    with st.sidebar.expander("🏆 賞一覧"):
        for pid, p in PRIZES.items():
            st.markdown(f"**{pid}**: {p['name']} ({p['min_amount']:,}円〜)")

    if mode == "📝 レシート入力":
        page_receipt_input(campaign, prize_id, user)
    elif mode == "📊 ダッシュボード":
        page_dashboard(campaign)


# ============================================================
# ダッシュボード（全賞横断）
# ============================================================
def page_dashboard(campaign: str):
    st.title("📊 ダッシュボード")

    try:
        all_stats = get_all_prize_stats(campaign)
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
        return

    # 全賞サマリーテーブル
    import pandas as pd

    rows = []
    total_all = 0
    done_all = 0
    for pid, stats in all_stats.items():
        if stats["total"] == 0:
            continue
        rows.append({
            "賞": f"{pid}: {PRIZES[pid]['name'][:20]}",
            "全件": stats["total"],
            "AI自動": stats["auto"],
            "要手入力": stats["needs_input"],
            "入力済": stats["human_done"],
            "進捗": f"{stats['progress']:.0%}",
        })
        total_all += stats["total"]
        done_all += stats["done"]

    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # 全体進捗
        overall = done_all / total_all if total_all > 0 else 0
        st.progress(overall)
        st.caption(f"全体進捗: {done_all}/{total_all} ({overall:.0%})")
    else:
        st.info(f"キャンペーン '{campaign}' にデータがありません。")
        return

    st.divider()

    # 作業中メンバー
    st.subheader("👥 作業中メンバー")
    all_workers = []
    for pid in PRIZES:
        try:
            workers = get_active_workers(campaign, pid)
            for w in workers:
                w["prize"] = pid
            all_workers.extend(workers)
        except Exception:
            pass

    if all_workers:
        for w in all_workers:
            st.markdown(
                f"- **{w['user']}** → {w['prize']}賞 #{w['display_key']} "
                f"（{w['minutes_ago']}分前）"
            )
    else:
        st.caption("現在作業中のメンバーはいません。")

    # 担当者別作業件数
    st.divider()
    st.subheader("📈 担当者別 作業件数")

    worker_stats = get_all_worker_stats(campaign)
    if worker_stats:
        # 賞IDリスト（データがある賞のみ）
        active_prizes = sorted(set(
            pid for member_stats in worker_stats.values()
            for pid in member_stats
        ))

        ws_rows = []
        for member, prize_counts in sorted(worker_stats.items()):
            row = {"担当者": member}
            total = 0
            for pid in active_prizes:
                count = prize_counts.get(pid, 0)
                row[f"{pid}賞"] = count
                total += count
            row["合計"] = total
            ws_rows.append(row)

        ws_df = pd.DataFrame(ws_rows)
        st.dataframe(ws_df, use_container_width=True, hide_index=True)

        # 棒グラフ
        chart_df = ws_df.set_index("担当者")[["合計"]]
        st.bar_chart(chart_df)
    else:
        st.caption("まだ作業実績がありません。")

    st.divider()

    # 賞別詳細
    st.subheader("🏆 賞別詳細")
    detail_prize = st.selectbox(
        "賞を選択",
        [pid for pid, s in all_stats.items() if s["total"] > 0],
        format_func=lambda pid: f"{pid}: {PRIZES[pid]['name']}",
        key="dashboard_prize_detail",
    )

    if detail_prize:
        _render_prize_detail(campaign, detail_prize, all_stats[detail_prize])


def _render_prize_detail(campaign: str, prize_id: str, stats: dict):
    """賞別の詳細統計"""
    from config import identify_store_group
    prize = PRIZES[prize_id]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("総件数", stats["total"])
    col2.metric("自動確定", stats["auto"])
    col3.metric("要手入力(残)", stats["needs_input"])
    col4.metric("入力完了", stats["human_done"])

    # 複数レシート応募者の情報
    entries = get_entries(campaign, prize_id)
    multi_receipt = [e for e in entries if e.get("receipt_count", 1) > 1]
    if multi_receipt:
        max_receipts = max(e.get("receipt_count", 1) for e in multi_receipt)
        st.info(f"📎 複数レシート応募者: {len(multi_receipt)}件（最大{max_receipts}枚）")
    else:
        st.caption(f"全{len(entries)}件（複数レシート応募なし）")

    ready = stats["needs_input"] == 0
    if ready:
        st.success("✅ 全件確認完了！ 抽選に進めます。")
    else:
        st.warning(f"⚠️ 残り {stats['needs_input']} 件の手入力が必要です。")

    # 資格判定
    st.divider()
    st.subheader(f"🏆 {prize_id}賞 資格判定")
    st.caption(
        f"条件: CP対象合計 ¥{prize['min_amount']:,} 以上"
        + (f" / {prize.get('store_group', '')}グループ限定" if prize.get("store_group") else "")
    )

    entries = get_entries(campaign, prize_id)
    eligible_count = 0
    ineligible_count = 0
    unreadable_count = 0
    pending_count = 0

    for e in entries:
        if not (e.get("is_auto") or e.get("human_input_done")):
            pending_count += 1
            continue
        if e.get("unreadable"):
            unreadable_count += 1
            continue
        cp_total = _get_entry_cp_total(e)
        store = _get_entry_store(e)
        ok, _ = check_eligibility(cp_total, store, prize_id)
        if ok:
            eligible_count += 1
        else:
            ineligible_count += 1

    col_e1, col_e2, col_e3, col_e4 = st.columns(4)
    col_e1.metric("✅ 資格あり", eligible_count)
    col_e2.metric("❌ 資格なし", ineligible_count)
    col_e3.metric("📛 読取不可", unreadable_count)
    col_e4.metric("⏳ 未確認", pending_count)

    st.caption(
        f"当選枠: {prize['winners']}名 / 予備: {prize['reserve']}名 "
        f"→ 資格あり {eligible_count}名 から抽選"
    )


def _get_entry_cp_total(entry: dict) -> int:
    if entry.get("is_auto") and not entry.get("human_input_done"):
        return entry.get("cp_target_total", 0)
    elif entry.get("human_input_done"):
        return entry.get("human_cp_total", 0)
    return 0


def _get_entry_store(entry: dict) -> str:
    if entry.get("is_auto") and not entry.get("human_input_done"):
        return entry.get("store_name", "")
    elif entry.get("human_input_done"):
        return entry.get("human_store_name", "")
    return ""


# ============================================================
# レシート入力画面
# ============================================================
def page_receipt_input(campaign: str, prize_id: str, user: str):
    entries = _get_cached_entries(campaign, prize_id)

    if not entries:
        st.info(f"データがありません: campaigns/{campaign}/prizes/{prize_id}")
        return

    # エントリ分類
    needs_input = []
    auto_confirmed = []
    human_done = []

    for e in entries:
        if e.get("is_auto") and not e.get("human_input_done"):
            auto_confirmed.append(e)
        elif e.get("human_input_done"):
            human_done.append(e)
        else:
            needs_input.append(e)

    # フィルタ切替（タブの代わりにradioで排他描画）
    view_options = [
        f"要手入力 ({len(needs_input)})",
        f"自動確定 ({len(auto_confirmed)})",
        f"入力済み ({len(human_done)})",
        f"全件 ({len(entries)})",
    ]
    selected_view = st.radio(
        "表示切替",
        view_options,
        horizontal=True,
        key="receipt_view_mode",
        label_visibility="collapsed",
    )

    if selected_view == view_options[0]:
        _render_needs_input(campaign, prize_id, user, needs_input, len(human_done))
    elif selected_view == view_options[1]:
        _render_auto_confirmed(auto_confirmed, campaign, prize_id, user)
    elif selected_view == view_options[2]:
        _render_human_done(campaign, prize_id, human_done, user)
    elif selected_view == view_options[3]:
        _render_all_entries(entries)


@st.cache_data(ttl=10)
def _get_cached_entries(campaign: str, prize_id: str) -> list[dict]:
    return get_entries(campaign, prize_id)


def _render_needs_input(
    campaign: str, prize_id: str, user: str,
    needs_input: list[dict], done_count: int,
):
    """要手入力エントリの入力"""
    if not needs_input:
        st.success("全件入力完了！ 抽選に進めます。")
        return

    # 進捗バー
    total_manual = len(needs_input) + done_count
    progress = done_count / total_manual if total_manual > 0 else 0
    st.progress(progress)
    st.caption(f"手入力進捗: {done_count}/{total_manual} ({progress:.0%})")

    with st.expander("📋 対象商品ガイド（判定の手引き）"):
        st.markdown("""
**キャンペーン対象**: 以下4カテゴリの「ビオレu」商品が対象です。

### ① ビオレu ザ ボディ（ボディウォッシュ）
泡タイプ・液体タイプ・ジェルタイプなど。本体/つめかえ/大容量つめかえ各種。
- ビオレu ザ ボディ 泡タイプ（各香り）
- ビオレu ザ ボディ 液体タイプ（各香り）
- ビオレu ザ ボディ ジェルタイプ
- ビオレu ぬれた肌に使う ボディ乳液（各香り）
- ビオレu 泡立つボディタオル 等

### ② ビオレu ザ ボディ 乳液（ボディ乳液）
- ビオレu ザ ボディ 乳液（各香り・各容量）

### ③ ビオレu ザ ボディ シャワー・道具
- ビオレu ザ ボディ 浴用タオル
- ビオレu ザ ボディ 泡立てネット 等

### ④ ビオレu ザ ハンド（ハンドソープ・ハンド乳液）
- ビオレu ザ ハンド 泡ハンドソープ（各香り）
- ビオレu ザ ハンド 泡スタンプ
- ビオレu ザ ハンド 乳液ハンドソープ 等

---
**💡 判定のコツ**
- レシート上では「ビオレUザボディ」「ﾋﾞｵﾚu」など省略表記の場合あり
- 「ビオレu」+「ザ ボディ」or「ザ ハンド」がキーワード
- 迷ったら「判断つかず」で入力、もしくは管理者にお問い合わせください
""")

    # 次のレシートを取得（排他制御付き）
    if st.button("🔄 次のレシートへ", type="primary", key="get_next"):
        # 現在のエントリのロックを解放してから次を取得
        current_entry = st.session_state.get("current_entry")
        if current_entry:
            release_entry(campaign, prize_id, current_entry["_id"])
        st.cache_data.clear()
        entry = get_next_unclaimed_entry(campaign, prize_id, user)
        if entry:
            st.session_state["current_entry"] = entry
        else:
            st.info("現在入力できるレシートがありません（他のメンバーが作業中か、すべて入力済みです）")

    # 現在のエントリ表示
    current = st.session_state.get("current_entry")
    if current is None:
        # 自動的に次のエントリを取得
        current = get_next_unclaimed_entry(campaign, prize_id, user)
        if current:
            st.session_state["current_entry"] = current

    if current is None:
        st.info("現在入力できるレシートがありません（他のメンバーが作業中か、すべて入力済みです）")
        return

    entry_id = current["_id"]
    filename = current.get("original_filename", Path(current.get("image_path", "")).name)
    display_key = get_entry_label(current)
    conf = current.get("confidence", 0)
    error = current.get("error")

    st.markdown(f"### 📝 #{display_key}")

    # 応募者情報（CSVインポート時）
    if current.get("last_name"):
        with st.expander("👤 応募者情報"):
            st.markdown(
                f"**氏名**: {current.get('last_name', '')} {current.get('first_name', '')}  \n"
                f"**〒**: {current.get('postal_code', '')} {current.get('prefecture', '')} "
                f"{current.get('city', '')} {current.get('address1', '')} "
                f"{current.get('address2', '')} {current.get('building', '')}  \n"
                f"**TEL**: {current.get('phone', '')}  \n"
                f"**コース**: {current.get('q2_course', '')}"
            )
    if error:
        st.warning("📛 AIがこのレシートを読み取れませんでした。レシート画像を見て入力してください。")
        with st.expander("エラー詳細（管理者向け）"):
            st.code(error)
    elif conf is not None:
        st.warning("⚠️ AIの読み取り精度が低いため、手入力をお願いします")

    # 複数レシート表示（images配列に含まれる）
    receipt_count = current.get("receipt_count", len(current.get("images", [])))
    if receipt_count > 1:
        st.info(f"📎 この応募者は{receipt_count}枚のレシートを提出しています")

    st.caption(f"担当: {user} | エントリID: {entry_id}")

    # 左右分割
    col_img, col_form = st.columns([1, 1])

    with col_img:
        st.subheader("📷 レシート画像")
        rotation_key = f"rotation_{entry_id}"
        zoom_key = f"zoom_{entry_id}"
        if rotation_key not in st.session_state:
            st.session_state[rotation_key] = 0
        if zoom_key not in st.session_state:
            st.session_state[zoom_key] = 100

        btn_cols = st.columns(5)
        with btn_cols[0]:
            if st.button("⬅ 左回転", key=f"rot_l_{entry_id}"):
                st.session_state[rotation_key] = (st.session_state[rotation_key] - 90) % 360
        with btn_cols[1]:
            if st.button("🔄 リセット", key=f"rot_r_{entry_id}"):
                st.session_state[rotation_key] = 0
                st.session_state[zoom_key] = 100
        with btn_cols[2]:
            if st.button("➡ 右回転", key=f"rot_rr_{entry_id}"):
                st.session_state[rotation_key] = (st.session_state[rotation_key] + 90) % 360
        with btn_cols[3]:
            if st.button("🔍-", key=f"zoom_out_{entry_id}"):
                st.session_state[zoom_key] = max(50, st.session_state[zoom_key] - 50)
        with btn_cols[4]:
            if st.button("🔍+", key=f"zoom_in_{entry_id}"):
                st.session_state[zoom_key] = min(400, st.session_state[zoom_key] + 50)

        zoom = st.session_state[zoom_key]
        if zoom != 100:
            st.caption(f"ズーム: {zoom}%")

        render_receipt_image(current, st.session_state[rotation_key], zoom)

    with col_form:
        st.subheader("📝 データ入力")

        # OCR結果を参考表示（折りたたみ）
        has_ocr = current.get("items") or any(
            img.get("ocr_done") for img in current.get("images", [])
        )
        if has_ocr and not current.get("error"):
            with st.expander("🤖 AI読み取り結果（参考）"):
                st.caption("⚠️ 信頼度が低いため人間確認が必要です。参考としてご利用ください。")

                images = current.get("images", [])
                ocr_images = [img for img in images if img.get("ocr_done")]

                if ocr_images:
                    # 新形式: images配列からOCR結果を表示
                    for img in ocr_images:
                        if len(ocr_images) > 1:
                            st.markdown(f"**--- レシート {img.get('receipt_number', '?')}枚目 ---**")
                        if img.get("store_name"):
                            st.markdown(f"**店舗**: {img.get('store_name', '')} {img.get('store_branch', '')}")
                        if img.get("purchase_date"):
                            st.markdown(f"**日時**: {img.get('purchase_date', '')}")

                        items = img.get("items", [])
                        cp_items_ref = [i for i in items if i.get("is_cp_target")]
                        kao_items_ref = [i for i in items if i.get("is_kao") and not i.get("is_cp_target")]

                        if cp_items_ref:
                            st.markdown("**🎯 CP対象品（AI判定）:**")
                            for item in cp_items_ref:
                                st.markdown(f"  - {item.get('name', '?')}  ¥{_safe_int(item.get('price', 0)):,}")

                        if kao_items_ref:
                            st.markdown("**🔵 その他花王（AI判定）:**")
                            for item in kao_items_ref:
                                st.markdown(f"  - {item.get('name', '?')}  ¥{_safe_int(item.get('price', 0)):,}")

                        st.markdown(f"**合計**: ¥{_safe_int(img.get('total', 0)):,}")
                else:
                    # 旧形式
                    if current.get("store_name"):
                        st.markdown(f"**店舗**: {current.get('store_name', '')} {current.get('store_branch', '')}")
                    if current.get("purchase_date"):
                        st.markdown(f"**日時**: {current.get('purchase_date', '')}")

                    items = current.get("items", [])
                    cp_items_ref = [i for i in items if i.get("is_cp_target")]
                    kao_items_ref = [i for i in items if i.get("is_kao") and not i.get("is_cp_target")]
                    other_items_ref = [i for i in items if not i.get("is_kao") and i.get("price", 0) > 0]

                    if cp_items_ref:
                        st.markdown("**🎯 CP対象品（AI判定）:**")
                        for item in cp_items_ref:
                            st.markdown(f"  - {item.get('name', '?')}  ¥{_safe_int(item.get('price', 0)):,}")

                    if kao_items_ref:
                        st.markdown("**🔵 その他花王（AI判定）:**")
                        for item in kao_items_ref:
                            st.markdown(f"  - {item.get('name', '?')}  ¥{_safe_int(item.get('price', 0)):,}")

                    if other_items_ref:
                        st.markdown("**📦 その他商品:**")
                        for item in other_items_ref[:5]:
                            st.markdown(f"  - {item.get('name', '?')}  ¥{_safe_int(item.get('price', 0)):,}")
                        if len(other_items_ref) > 5:
                            st.caption(f"  ...他{len(other_items_ref) - 5}件")

                    st.markdown(f"**合計**: ¥{_safe_int(current.get('total', 0)):,}")

        form_data = _render_entry_form(current, entry_id)

        confirm_key = f"confirm_save_{entry_id}"

        if st.button(
            "💾 保存して次のレシートへ",
            type="primary",
            use_container_width=True,
            key=f"save_{entry_id}",
        ):
            st.session_state.pop(confirm_key, None)

            warnings = _get_validation_warnings(form_data)
            if warnings:
                st.session_state[confirm_key] = warnings
                st.rerun()
            else:
                _save_entry_to_firestore(campaign, prize_id, entry_id, form_data, user)
                st.cache_data.clear()
                # 次のエントリを取得
                next_entry = get_next_unclaimed_entry(campaign, prize_id, user)
                st.session_state["current_entry"] = next_entry
                st.rerun()

        # バリデーション確認ダイアログ
        if confirm_key in st.session_state:
            for w in st.session_state[confirm_key]:
                st.warning(w)
            col_yes, col_no = st.columns(2)
            with col_yes:
                if st.button("✅ はい、このまま保存", key=f"confirm_yes_{entry_id}"):
                    del st.session_state[confirm_key]
                    _save_entry_to_firestore(campaign, prize_id, entry_id, form_data, user)
                    st.cache_data.clear()
                    next_entry = get_next_unclaimed_entry(campaign, prize_id, user)
                    st.session_state["current_entry"] = next_entry
                    st.rerun()
            with col_no:
                if st.button("↩ いいえ、戻る", key=f"confirm_no_{entry_id}"):
                    del st.session_state[confirm_key]
                    st.rerun()

        # スキップボタン
        if st.button("⏭ このレシートは後で入力する", key=f"skip_{entry_id}"):
            release_entry(campaign, prize_id, entry_id)
            st.cache_data.clear()
            next_entry = get_next_unclaimed_entry(campaign, prize_id, user)
            st.session_state["current_entry"] = next_entry
            st.rerun()


def _render_auto_confirmed(entries: list[dict], campaign: str = "", prize_id: str = "", user: str = ""):
    """自動確定エントリの閲覧・修正"""
    if not entries:
        st.info("自動確定エントリはありません。")
        return

    st.caption(f"信頼度 {CONFIDENCE_THRESHOLD} 以上のエントリ（修正可能）")

    entries_sorted = sorted(entries, key=lambda e: (e.get("form_id", 0), e.get("answer_id", extract_filename_number(e))))

    options = [
        f"#{get_entry_label(e)} "
        f"(conf: {e.get('confidence', 0):.2f})"
        for e in entries_sorted
    ]
    selected = st.selectbox("エントリ選択", options, key="auto_select")
    if selected is None:
        return

    sel_idx = options.index(selected)
    entry = entries_sorted[sel_idx]
    entry_id = entry.get("_id", "")

    # 編集モード管理
    edit_key = f"auto_editing_{entry_id}"
    is_editing = st.session_state.get(edit_key, False)

    col_img, col_data = st.columns([1, 1])

    with col_img:
        st.subheader("📷 レシート画像")
        rotation_key = f"auto_rotation_{entry_id}"
        zoom_key = f"auto_zoom_{entry_id}"
        if rotation_key not in st.session_state:
            st.session_state[rotation_key] = 0
        if zoom_key not in st.session_state:
            st.session_state[zoom_key] = 100

        btn_cols = st.columns(5)
        with btn_cols[0]:
            if st.button("⬅ 左回転", key=f"auto_rot_l_{entry_id}"):
                st.session_state[rotation_key] = (st.session_state[rotation_key] - 90) % 360
        with btn_cols[1]:
            if st.button("🔄 リセット", key=f"auto_rot_r_{entry_id}"):
                st.session_state[rotation_key] = 0
                st.session_state[zoom_key] = 100
        with btn_cols[2]:
            if st.button("➡ 右回転", key=f"auto_rot_rr_{entry_id}"):
                st.session_state[rotation_key] = (st.session_state[rotation_key] + 90) % 360
        with btn_cols[3]:
            if st.button("🔍-", key=f"auto_zoom_out_{entry_id}"):
                st.session_state[zoom_key] = max(50, st.session_state[zoom_key] - 50)
        with btn_cols[4]:
            if st.button("🔍+", key=f"auto_zoom_in_{entry_id}"):
                st.session_state[zoom_key] = min(400, st.session_state[zoom_key] + 50)

        zoom = st.session_state[zoom_key]
        if zoom != 100:
            st.caption(f"ズーム: {zoom}%")

        render_receipt_image(entry, st.session_state[rotation_key], zoom)

    with col_data:
        if is_editing:
            # --- 編集モード ---
            st.subheader("✏️ AI自動確定の修正")
            st.warning("⚠️ AI判定を人間が修正します。保存後は「入力済み」に移動します。")

            # OCR結果をプリセットとしてフォーム表示
            # human_* がまだなければOCR結果をデフォルト値に
            edit_entry = dict(entry)
            if not edit_entry.get("human_store_name"):
                edit_entry["human_store_name"] = entry.get("store_name", "")
            if not edit_entry.get("human_store_branch"):
                edit_entry["human_store_branch"] = entry.get("store_branch", "")
            if not edit_entry.get("human_purchase_date"):
                edit_entry["human_purchase_date"] = entry.get("purchase_date", "")
            if not edit_entry.get("human_cp_items"):
                edit_entry["human_cp_items"] = [
                    item for item in entry.get("items", []) if item.get("is_cp_target")
                ]
            if not edit_entry.get("human_kao_items"):
                edit_entry["human_kao_items"] = [
                    item for item in entry.get("items", [])
                    if item.get("is_kao") and not item.get("is_cp_target")
                ]

            form_data = _render_entry_form(edit_entry, entry_id, key_prefix="autoedit_")

            edit_confirm_key = f"autoedit_confirm_save_{entry_id}"

            col_save, col_cancel = st.columns(2)
            with col_save:
                if st.button(
                    "💾 保存",
                    type="primary",
                    use_container_width=True,
                    key=f"autoedit_save_{entry_id}",
                ):
                    st.session_state.pop(edit_confirm_key, None)
                    warnings = _get_validation_warnings(form_data)
                    if warnings:
                        st.session_state[edit_confirm_key] = warnings
                        st.rerun()
                    else:
                        _save_entry_to_firestore(campaign, prize_id, entry_id, form_data, user)
                        st.cache_data.clear()
                        _cleanup_edit_state(entry_id, edit_key)
                        st.rerun()
            with col_cancel:
                if st.button(
                    "❌ キャンセル",
                    use_container_width=True,
                    key=f"autoedit_cancel_{entry_id}",
                ):
                    st.session_state.pop(edit_confirm_key, None)
                    _cleanup_edit_state(entry_id, edit_key)
                    st.rerun()

            if edit_confirm_key in st.session_state:
                for w in st.session_state[edit_confirm_key]:
                    st.warning(w)
                col_yes, col_no = st.columns(2)
                with col_yes:
                    if st.button("✅ はい、このまま保存", key=f"autoedit_confirm_yes_{entry_id}"):
                        del st.session_state[edit_confirm_key]
                        _save_entry_to_firestore(campaign, prize_id, entry_id, form_data, user)
                        st.cache_data.clear()
                        _cleanup_edit_state(entry_id, edit_key)
                        st.rerun()
                with col_no:
                    if st.button("↩ いいえ、戻る", key=f"autoedit_confirm_no_{entry_id}"):
                        del st.session_state[edit_confirm_key]
                        st.rerun()
        else:
            # --- 閲覧モード ---
            st.subheader("📋 AI読み取り結果")
            conf = entry.get("confidence", 0)
            st.success(f"✅ 信頼度: {conf:.2f} (AI自動確定)")

            if st.button("✏️ 修正する", key=f"auto_edit_btn_{entry_id}"):
                st.session_state[edit_key] = True
                st.rerun()

            # 新形式（images配列）の場合はレシートごとに表示
            images = entry.get("images", [])
            if images and images[0].get("ocr_done"):
                total_cp = 0
                for img in images:
                    if len(images) > 1:
                        st.markdown(f"---\n**レシート {img.get('receipt_number', '?')}枚目**")
                    st.markdown(f"**店舗**: {img.get('store_name', '不明')} {img.get('store_branch', '')}")
                    st.markdown(f"**日時**: {img.get('purchase_date', '不明')}")
                    st.markdown(f"**税区分**: {img.get('tax_type', '不明')}")
                    if img.get("tax_adjusted"):
                        st.caption("※ 税補正済")

                    cp_items = [i for i in img.get("items", []) if i.get("is_cp_target")]
                    if cp_items:
                        st.markdown("**🎯 CP対象品:**")
                        for i in cp_items:
                            st.markdown(f"  - {i.get('name', '?')}  ¥{_safe_int(i.get('price', 0)):,}")
                        img_cp = img.get("cp_target_total", 0)
                        st.markdown(f"  **CP小計: ¥{img_cp:,}**")
                        total_cp += img_cp
                    else:
                        st.markdown("**🎯 CP対象品:** （なし）")

                    kao_items = [i for i in img.get("items", []) if i.get("is_kao") and not i.get("is_cp_target")]
                    if kao_items:
                        st.markdown("**🔵 その他花王製品:**")
                        for i in kao_items:
                            st.markdown(f"  - {i.get('name', '?')}  ¥{_safe_int(i.get('price', 0)):,}")

                if len(images) > 1:
                    st.divider()
                    st.markdown(f"**CP合計（全レシート）: ¥{total_cp:,}**")
            else:
                # 旧形式 or OCR未実行
                st.markdown(f"**店舗**: {entry.get('store_name', '不明')} {entry.get('store_branch', '')}")
                st.markdown(f"**日時**: {entry.get('purchase_date', '不明')}")
                st.markdown(f"**税区分**: {entry.get('tax_type', '不明')}")
                if entry.get("tax_adjusted"):
                    st.caption("※ 税補正済")

                st.divider()

                cp_items = [item for item in entry.get("items", []) if item.get("is_cp_target")]
                if cp_items:
                    st.markdown("**🎯 CP対象品:**")
                    for item in cp_items:
                        price = item.get("price", 0)
                        st.markdown(f"  - {item.get('name', '?')}  ¥{price:,}")
                    cp_total = entry.get("cp_target_total", 0)
                    st.markdown(f"  **CP合計: ¥{cp_total:,}**")
                else:
                    st.markdown("**🎯 CP対象品:** （なし）")

                kao_items = [
                    item for item in entry.get("items", [])
                    if item.get("is_kao") and not item.get("is_cp_target")
                ]
                if kao_items:
                    st.markdown("**🔵 その他花王製品:**")
                    for item in kao_items:
                        price = item.get("price", 0)
                        st.markdown(f"  - {item.get('name', '?')}  ¥{price:,}")

            st.divider()
            eligibility = get_eligibility_display(entry.get("cp_target_total", 0))
            st.info(f"→ {eligibility}")


def _render_human_done(campaign: str, prize_id: str, entries: list[dict], user: str = ""):
    """入力済みエントリの閲覧・編集"""
    if not entries:
        st.info("入力済みエントリはありません。")
        return

    # 対応者で絞り込み
    workers = sorted(set(e.get("completed_by", "") for e in entries if e.get("completed_by")))
    filter_options = ["すべて"] + workers
    selected_worker = st.selectbox("対応者で絞り込み", filter_options, key="done_worker_filter")

    if selected_worker != "すべて":
        filtered = [e for e in entries if e.get("completed_by") == selected_worker]
    else:
        filtered = entries

    st.caption(f"入力完了分 ({len(filtered)}件" + (f" / 全{len(entries)}件)" if selected_worker != "すべて" else ")"))

    if not filtered:
        st.info("該当するレシートがありません。")
        return

    options = [
        f"#{get_entry_label(e)} "
        f"— CP: ¥{e.get('human_cp_total', 0):,}"
        for e in filtered
    ]
    selected = st.selectbox("エントリ選択", options, key="done_select")
    if selected is None:
        return

    sel_idx = options.index(selected)
    entry = filtered[sel_idx]
    entry_id = entry["_id"]

    edit_key = f"editing_{entry_id}"
    is_editing = st.session_state.get(edit_key, False)

    col_img, col_data = st.columns([1, 1])

    with col_img:
        st.subheader("📷 レシート画像")
        rotation_key = f"done_rotation_{entry_id}"
        zoom_key = f"done_zoom_{entry_id}"
        if rotation_key not in st.session_state:
            st.session_state[rotation_key] = 0
        if zoom_key not in st.session_state:
            st.session_state[zoom_key] = 100

        btn_cols = st.columns(5)
        with btn_cols[0]:
            if st.button("⬅ 左回転", key=f"done_rot_l_{entry_id}"):
                st.session_state[rotation_key] = (st.session_state[rotation_key] - 90) % 360
        with btn_cols[1]:
            if st.button("🔄 リセット", key=f"done_rot_r_{entry_id}"):
                st.session_state[rotation_key] = 0
                st.session_state[zoom_key] = 100
        with btn_cols[2]:
            if st.button("➡ 右回転", key=f"done_rot_rr_{entry_id}"):
                st.session_state[rotation_key] = (st.session_state[rotation_key] + 90) % 360
        with btn_cols[3]:
            if st.button("🔍-", key=f"done_zoom_out_{entry_id}"):
                st.session_state[zoom_key] = max(50, st.session_state[zoom_key] - 50)
        with btn_cols[4]:
            if st.button("🔍+", key=f"done_zoom_in_{entry_id}"):
                st.session_state[zoom_key] = min(400, st.session_state[zoom_key] + 50)

        zoom = st.session_state[zoom_key]
        if zoom != 100:
            st.caption(f"ズーム: {zoom}%")

        render_receipt_image(entry, st.session_state[rotation_key], zoom)

    with col_data:
        if is_editing:
            st.subheader("✏️ データ編集")

            form_data = _render_entry_form(entry, entry_id, key_prefix="edit_")

            edit_confirm_key = f"edit_confirm_save_{entry_id}"

            col_save, col_cancel = st.columns(2)
            with col_save:
                if st.button(
                    "💾 保存",
                    type="primary",
                    use_container_width=True,
                    key=f"edit_save_{entry_id}",
                ):
                    st.session_state.pop(edit_confirm_key, None)
                    warnings = _get_validation_warnings(form_data)
                    if warnings:
                        st.session_state[edit_confirm_key] = warnings
                        st.rerun()
                    else:
                        _save_entry_to_firestore(campaign, prize_id, entry_id, form_data, user)
                        st.cache_data.clear()
                        _cleanup_edit_state(entry_id, edit_key)
                        st.rerun()
            with col_cancel:
                if st.button(
                    "❌ キャンセル",
                    use_container_width=True,
                    key=f"edit_cancel_{entry_id}",
                ):
                    st.session_state.pop(edit_confirm_key, None)
                    _cleanup_edit_state(entry_id, edit_key)
                    st.rerun()

            if edit_confirm_key in st.session_state:
                for w in st.session_state[edit_confirm_key]:
                    st.warning(w)
                col_yes, col_no = st.columns(2)
                with col_yes:
                    if st.button("✅ はい、このまま保存", key=f"edit_confirm_yes_{entry_id}"):
                        del st.session_state[edit_confirm_key]
                        _save_entry_to_firestore(campaign, prize_id, entry_id, form_data, user)
                        st.cache_data.clear()
                        _cleanup_edit_state(entry_id, edit_key)
                        st.rerun()
                with col_no:
                    if st.button("↩ いいえ、戻る", key=f"edit_confirm_no_{entry_id}"):
                        del st.session_state[edit_confirm_key]
                        st.rerun()
        else:
            st.subheader("📋 入力データ")

            if st.button("✏️ 編集する", key=f"edit_btn_{entry_id}"):
                st.session_state[edit_key] = True
                st.rerun()

            if entry.get("unreadable"):
                st.error("📛 レシート読み取り不可")
            else:
                st.markdown(
                    f"**店舗**: {entry.get('human_store_name', '')} "
                    f"{entry.get('human_store_branch', '')}"
                )
                st.markdown(f"**日時**: {entry.get('human_purchase_date', '')}")

                cp_items = entry.get("human_cp_items", [])
                if cp_items:
                    st.markdown("**🎯 CP対象品:**")
                    for item in cp_items:
                        st.markdown(f"  - {item['name']}  ¥{item['price']:,}")
                    st.markdown(f"  **CP合計: ¥{entry.get('human_cp_total', 0):,}**")

                kao_items = entry.get("human_kao_items", [])
                if kao_items:
                    st.markdown("**🔵 その他花王:**")
                    for item in kao_items:
                        st.markdown(f"  - {item['name']}  ¥{item['price']:,}")

                unknown_items = entry.get("human_unknown_items", [])
                if unknown_items:
                    st.markdown("**❓ 判断つかず:**")
                    for item in unknown_items:
                        st.markdown(f"  - {item['name']}  ¥{item['price']:,}")

                st.divider()
                eligibility = get_eligibility_display(entry.get("human_cp_total", 0))
                st.info(f"→ {eligibility}")


def _render_all_entries(entries: list[dict]):
    """全件一覧表示"""
    import pandas as pd

    rows = []
    for e in entries:
        conf = e.get("confidence", 0)
        error = e.get("error")

        if e.get("is_auto") and not e.get("human_input_done"):
            status = "✅ 自動確定"
            cp_total = e.get("cp_target_total", 0)
            store = e.get("store_name", "")
        elif e.get("human_input_done"):
            status = "📝 入力済み"
            cp_total = e.get("human_cp_total", 0)
            store = e.get("human_store_name", "")
        elif error:
            status = "❌ エラー"
            cp_total = 0
            store = ""
        else:
            status = "⏳ 要手入力"
            cp_total = 0
            store = ""

        assigned = e.get("assigned_to", "")
        row = {
            "ID": get_entry_label(e),
            "信頼度": f"{conf:.2f}" if conf and not error else ("ERR" if error else "-"),
            "状態": status,
            "レシート数": e.get("receipt_count", 1),
            "店舗": store,
            "CP合計": f"¥{cp_total:,}" if cp_total else "",
            "担当": assigned or "",
        }
        # 応募者情報があれば表示
        if e.get("last_name"):
            row["応募者"] = f"{e.get('last_name', '')} {e.get('first_name', '')}"
        rows.append(row)

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
