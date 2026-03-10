"""
花王CP レシートチェックv2 - 設定
信頼度ベース分岐 + 管理者抜き打ちチェック + Firebase対応
"""

# ==== 信頼度閾値 ====
CONFIDENCE_THRESHOLD = 0.90  # 90%以上 → AI自動確定、未満 → 人間手入力
ADMIN_CHECK_RATIO = 0.15     # 人間入力分の15%を抜き打ちチェック

# ==== マルチユーザー設定 ====
TEAM_MEMBERS = ["新田", "田中", "佐藤", "鈴木", "山田"]
DEFAULT_CAMPAIGN_ID = "kao_cp_2026"
LOCK_TIMEOUT_MINUTES = 30

# ==== 賞の設定 ====
PRIZES = {
    "S": {
        "name": "S賞（TDSホテル宿泊+パスポート）",
        "min_amount": 10000,
        "winners": 2,
        "reserve": 3,
        "deadline": "2026-03-17",
        "store_group": None,
    },
    "A": {
        "name": "A賞（TDS25周年スペシャルナイトパスポート）",
        "min_amount": 1700,
        "winners": 25,
        "reserve": 10,
        "deadline": "2026-03-17",
        "store_group": None,
    },
    "B": {
        "name": "B賞（TDRパークチケット）",
        "min_amount": 1700,
        "winners": 25,
        "reserve": 10,
        "deadline": "2026-03-17",
        "store_group": None,
    },
    "C": {
        "name": "C賞（ビオレザハンド ディズニーデザイン3種セット）",
        "min_amount": 600,
        "winners": 1000,
        "reserve": 50,
        "deadline": "2026-03-24",
        "store_group": None,
    },
    "SP_TSURUHA": {
        "name": "スペシャルグッズコース（ツルハグループ）",
        "min_amount": 600,
        "winners": 500,
        "reserve": 20,
        "deadline": "2026-03-24",
        "store_group": "tsuruha",
    },
    "SP_WELCIA": {
        "name": "スペシャルグッズコース（ウエルシアグループ）",
        "min_amount": 600,
        "winners": 500,
        "reserve": 20,
        "deadline": "2026-03-24",
        "store_group": "welcia",
    },
}

# ==== 店舗グループ ====
STORE_GROUPS = {
    "tsuruha": [
        "ツルハドラッグ", "ツルハ",
        "B&Dドラッグストア", "B&D",
        "くすりの福太郎", "福太郎",
        "ドラッグストアウェルネス", "ウェルネス",
        "ウォンツ",
        "くすりのレディ", "レディ薬局",
        "杏林堂",
        "ドラッグイレブン",
    ],
    "welcia": [
        "ウエルシア薬局", "ウエルシア", "welcia",
        "ハックドラッグ", "HAC",
        "金光薬品",
        "ダックス",
        "ハッピー・ドラッグ", "ハッピードラッグ",
        "よどやドラッグ", "よどや",
        "マルエドラッグ",
        "コクミン",
        "スーパードラッグひまわり", "ひまわり",
        "ふく薬品",
        "ウェルパーク",
    ],
}

# ==== 納品CSVカラム数 ====
CSV_COLUMNS = {
    "cp_target_max": 6,      # CP対象品 ①〜⑥
    "kao_other_max": 10,      # その他花王製品 ①〜⑩
    "unknown_kao_max": 5,     # 判断つかず花王製品 ①〜⑤
}


def get_prize(prize_id: str) -> dict:
    if prize_id not in PRIZES:
        raise ValueError(f"不明な賞ID: {prize_id}。有効な値: {list(PRIZES.keys())}")
    return PRIZES[prize_id]


def identify_store_group(store_name: str) -> str | None:
    if not store_name:
        return None
    store_lower = store_name.lower()
    for group_id, stores in STORE_GROUPS.items():
        for store in stores:
            if store.lower() in store_lower:
                return group_id
    return None


def check_eligibility(amount: int, store_name: str, prize_id: str) -> tuple[bool, str]:
    prize = get_prize(prize_id)
    if amount < prize["min_amount"]:
        return False, f"金額不足（{amount:,}円 < {prize['min_amount']:,}円）"
    if prize["store_group"]:
        store_group = identify_store_group(store_name)
        if store_group != prize["store_group"]:
            expected = "ツルハグループ" if prize["store_group"] == "tsuruha" else "ウエルシアグループ"
            return False, f"店舗条件不一致（{expected}限定）"
    return True, "OK"


# ==== CSV列マッピング（import_csv.py用） ====
CSV_COLUMNS_MAP = {
    "form_id": "フォームID",
    "answer_id": "回答ID",
    "answered_at": "回答日時",
    "updated_at_csv": "更新日時",
    "q1_confirm": "Q1.応募規約に記載の【ビオレ ザ ボディ】【ビオレ ザ ハンド】シリーズ 関連商品を対象金額以上ご購入いただいた方が対象です。 条件を満たしているか、再度ご確認いただけますでしょうか。",
    "q2_course": "Q2.ご希望の応募コースを選択してください。",
    "receipt_1": "Q3.レシートの写真をアップロードしてください。（１枚目）",
    "receipt_2": "Q3 レシートの写真をアップロードしてください。（２枚目）",
    "receipt_3": "Q3 レシートの写真をアップロードしてください。（３枚目）",
    "receipt_4": "Q3 レシートの写真をアップロードしてください。（４枚目）",
    "receipt_5": "Q3 レシートの写真をアップロードしてください。（５枚目）",
    "receipt_6": "Q3 レシートの写真をアップロードしてください。（６枚目）",
    "receipt_7": "Q3 レシートの写真をアップロードしてください。（７枚目）",
    "q4_source": "Q4.どこでこのキャンペーンを知りましたか？",
    "q4_source_other": "Q4.どこでこのキャンペーンを知りましたか？(その他テキスト)",
    "last_name": "お名前(姓)",
    "first_name": "お名前(名)",
    "postal_code": "郵便番号",
    "prefecture": "都道府県",
    "city": "市区町村",
    "address1": "市区町村以下",
    "address2": "番地",
    "building": "ビル建物名",
    "phone": "電話番号",
    "email": "メールアドレス",
    "age": "年齢",
    "gender": "性別",
}


def check_eligibility_group(
    cp_total: int,
    store_groups: list[str | None],
    prize_id: str,
) -> tuple[bool, str]:
    """
    複数レシート合算版の資格判定。
    cp_total: 合算後のCP対象品合計
    store_groups: 各レシートの店舗グループ識別結果のリスト
    prize_id: 賞ID
    """
    prize = get_prize(prize_id)
    if cp_total < prize["min_amount"]:
        return False, f"金額不足（{cp_total:,}円 < {prize['min_amount']:,}円）"
    if prize["store_group"]:
        # 1枚でも該当店舗グループがあればOK
        if not any(sg == prize["store_group"] for sg in store_groups):
            expected = "ツルハグループ" if prize["store_group"] == "tsuruha" else "ウエルシアグループ"
            return False, f"店舗条件不一致（{expected}限定）"
    return True, "OK"
