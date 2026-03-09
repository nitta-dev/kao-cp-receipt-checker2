"""
花王CP レシートチェックv2 - 商品マスター管理・あいまい検索
POS表記ブレに対応するため、正規化＋部分一致＋fuzzyマッチを組み合わせる
"""
import json
import re
import unicodedata
from pathlib import Path

try:
    from rapidfuzz import fuzz, process
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False

DATA_DIR = Path(__file__).parent / "data"


def _normalize(text: str) -> str:
    """テキストを正規化: 全角→半角、スペース除去、カタカナ統一"""
    if not text:
        return ""
    # NFKC正規化（全角英数→半角、半角カナ→全角カナ等）
    text = unicodedata.normalize("NFKC", text)
    # スペース・記号除去
    text = re.sub(r"[\s　・/／\-\(\)（）\[\]【】]", "", text)
    # 小文字化
    text = text.lower()
    return text


def _katakana_to_hiragana(text: str) -> str:
    """カタカナをひらがなに変換（比較用）"""
    return "".join(
        chr(ord(ch) - 0x60) if "\u30A1" <= ch <= "\u30F6" else ch
        for ch in text
    )


class ProductMaster:
    """商品マスター。CP対象・花王その他を統合管理し、あいまい検索を提供"""

    def __init__(self):
        self.cp_products: list[dict] = []
        self.kao_brands: list[dict] = []
        self._search_index: list[tuple[str, dict, str]] = []  # (正規化テキスト, product, source)
        self._load()

    def _load(self):
        cp_path = DATA_DIR / "cp_target_master.json"
        kao_path = DATA_DIR / "kao_other_master.json"

        if cp_path.exists():
            data = json.loads(cp_path.read_text(encoding="utf-8"))
            self.cp_products = data.get("products", [])

        if kao_path.exists():
            data = json.loads(kao_path.read_text(encoding="utf-8"))
            self.kao_brands = data.get("brands", [])

        self._build_index()

    def _build_index(self):
        """検索用インデックスを構築"""
        self._search_index = []

        for prod in self.cp_products:
            # 公式名
            self._search_index.append((
                _normalize(prod["official_name"]),
                prod,
                "cp_target"
            ))
            # 略称
            if prod.get("short_name"):
                self._search_index.append((
                    _normalize(prod["short_name"]),
                    prod,
                    "cp_target"
                ))
            # エイリアス
            for alias in prod.get("aliases", []):
                self._search_index.append((
                    _normalize(alias),
                    prod,
                    "cp_target"
                ))

        for brand in self.kao_brands:
            brand_entry = {
                "official_name": brand["brand"],
                "brand": brand["brand"],
                "category": brand.get("category", ""),
            }
            self._search_index.append((
                _normalize(brand["brand"]),
                brand_entry,
                "kao_other"
            ))
            for alias in brand.get("aliases", []):
                self._search_index.append((
                    _normalize(alias),
                    brand_entry,
                    "kao_other"
                ))

    def search(self, query: str, limit: int = 10) -> list[dict]:
        """
        あいまい検索。以下の優先順で結果を返す:
        1. 正規化テキストの部分一致（最も高速・確実）
        2. ひらがな変換後の部分一致（カタカナ/ひらがな揺れ対応）
        3. rapidfuzz によるfuzzyマッチ（POS省略表記対応）

        返り値: [{"name": str, "source": "cp_target"|"kao_other", "score": float, "product": dict}]
        """
        if not query or not query.strip():
            return []

        norm_query = _normalize(query)
        hira_query = _katakana_to_hiragana(norm_query)

        results = []
        seen = set()

        # Phase 1: 正規化テキスト部分一致
        for norm_text, product, source in self._search_index:
            if norm_query in norm_text or norm_text in norm_query:
                key = (product.get("official_name", product.get("brand", "")), source)
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "name": product.get("official_name", product.get("brand", "")),
                        "source": source,
                        "score": 100.0,
                        "product": product,
                    })

        # Phase 2: ひらがな変換部分一致
        if len(results) < limit:
            for norm_text, product, source in self._search_index:
                hira_text = _katakana_to_hiragana(norm_text)
                if hira_query in hira_text or hira_text in hira_query:
                    key = (product.get("official_name", product.get("brand", "")), source)
                    if key not in seen:
                        seen.add(key)
                        results.append({
                            "name": product.get("official_name", product.get("brand", "")),
                            "source": source,
                            "score": 90.0,
                            "product": product,
                        })

        # Phase 3: rapidfuzz（部分一致で足りない場合）
        if len(results) < limit and HAS_RAPIDFUZZ and len(norm_query) >= 2:
            candidates = [
                (norm_text, product, source)
                for norm_text, product, source in self._search_index
            ]
            texts = [c[0] for c in candidates]

            fuzzy_results = process.extract(
                norm_query, texts,
                scorer=fuzz.partial_ratio,
                limit=limit * 2,
                score_cutoff=50,
            )
            for match_text, score, idx in fuzzy_results:
                _, product, source = candidates[idx]
                key = (product.get("official_name", product.get("brand", "")), source)
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "name": product.get("official_name", product.get("brand", "")),
                        "source": source,
                        "score": score * 0.8,  # fuzzyは信頼度を下げる
                        "product": product,
                    })

        # CP対象を優先、スコア降順でソート
        results.sort(key=lambda r: (
            0 if r["source"] == "cp_target" else 1,
            -r["score"],
        ))
        return results[:limit]

    def classify_product(self, product_name: str) -> str:
        """
        商品名から分類を推定:
        - "cp_target": CP対象商品
        - "kao_other": その他花王製品
        - "unknown": 判断つかず
        """
        results = self.search(product_name, limit=1)
        if not results:
            return "unknown"
        top = results[0]
        if top["score"] >= 80 and top["source"] == "cp_target":
            return "cp_target"
        elif top["score"] >= 80 and top["source"] == "kao_other":
            return "kao_other"
        return "unknown"

    def get_cp_product_names(self) -> list[str]:
        """CP対象商品の公式名一覧（UIのドロップダウン用）"""
        return [p["official_name"] for p in self.cp_products]

    def get_kao_brand_names(self) -> list[str]:
        """花王ブランド名一覧（UIのドロップダウン用）"""
        return [b["brand"] for b in self.kao_brands]

    def add_alias(self, official_name: str, new_alias: str) -> bool:
        """運用中に発見した新しいPOS表記パターンをaliasに追加"""
        for prod in self.cp_products:
            if prod["official_name"] == official_name:
                if new_alias not in prod.get("aliases", []):
                    prod.setdefault("aliases", []).append(new_alias)
                    self._save_cp_master()
                    self._build_index()
                    return True
        return False

    def _save_cp_master(self):
        """CP対象マスターをファイルに保存"""
        cp_path = DATA_DIR / "cp_target_master.json"
        existing = json.loads(cp_path.read_text(encoding="utf-8"))
        existing["products"] = self.cp_products
        cp_path.write_text(
            json.dumps(existing, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


# シングルトン
_master: ProductMaster | None = None


def get_master() -> ProductMaster:
    global _master
    if _master is None:
        _master = ProductMaster()
    return _master
