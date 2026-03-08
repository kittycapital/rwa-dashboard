"""
RWA 토큰화 자산 데이터 수집 스크립트
DefiLlama 무료 API → CSV 파일 생성
GitHub Actions daily cron으로 자동 실행

생성 파일:
  data/rwa_tvl_history.csv      — RWA 전체 TVL 일별 추이
  data/rwa_category_history.csv — 카테고리별 TVL 일별 추이
  data/rwa_protocols.csv        — 프로토콜 순위 (현재 스냅샷)
  data/rwa_chains.csv           — 체인별 분포 (현재 스냅샷)
  data/rwa_top_protocols_history.csv — Top N 프로토콜 TVL 히스토리
  data/defi_total_tvl.csv       — DeFi 전체 TVL (비율 계산용)
"""

import requests
import csv
import os
import time
import json
from datetime import datetime, timezone
from collections import defaultdict

# ============================================================
# CONFIG
# ============================================================
BASE_URL = "https://api.llama.fi"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
TOP_N_PROTOCOLS = 15  # 히스토리 추적할 프로토콜 수
REQUEST_DELAY = 0.5   # API rate limit 방지 (초)

# RWA 프로토콜 → 카테고리 매핑 (수동 분류)
# DefiLlama에 세부 카테고리가 없으므로 주요 프로토콜은 수동 매핑
CATEGORY_MAP = {
    # Tokenized Treasuries
    "hashnote": "Treasuries",
    "ondo-finance": "Treasuries",
    "blackrock-buidl": "Treasuries",
    "franklin-templeton": "Treasuries",
    "matrixdock": "Treasuries",
    "superstate": "Treasuries",
    "openeden": "Treasuries",
    "securitize": "Treasuries",
    "mountain-protocol": "Treasuries",
    "backed-finance": "Treasuries",
    "midas": "Treasuries",
    "arca": "Treasuries",
    "adapt3r": "Treasuries",
    "hashnote-usyc": "Treasuries",
    "usdy": "Treasuries",
    "spiko": "Treasuries",
    # Private Credit
    "maple": "Private Credit",
    "centrifuge": "Private Credit",
    "goldfinch": "Private Credit",
    "credix-finance": "Private Credit",
    "clearpool": "Private Credit",
    "truefi": "Private Credit",
    "jia": "Private Credit",
    "homecoin": "Private Credit",
    "florence-finance": "Private Credit",
    # Commodities
    "paxos-gold": "Commodities",
    "tether-gold": "Commodities",
    "cache-gold": "Commodities",
    "comtech-gold": "Commodities",
    # Real Estate
    "realt": "Real Estate",
    "tangible": "Real Estate",
    "landx": "Real Estate",
    "parcl": "Real Estate",
    "citadao": "Real Estate",
    "lofty-ai": "Real Estate",
}

# ============================================================
# HELPERS
# ============================================================
def api_get(endpoint, retries=3):
    """DefiLlama API GET 요청 (재시도 로직 포함)"""
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"  ⚠ Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  ⚠ HTTP {resp.status_code} for {endpoint}")
                time.sleep(2)
        except Exception as e:
            print(f"  ⚠ Error: {e}")
            time.sleep(3)
    return None


def ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def save_csv(filename, headers, rows):
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  ✅ {filename} ({len(rows)} rows)")


def ts_to_date(ts):
    """Unix timestamp → YYYY-MM-DD"""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def get_category(slug):
    """프로토콜 slug → 카테고리 반환"""
    slug_lower = slug.lower()
    if slug_lower in CATEGORY_MAP:
        return CATEGORY_MAP[slug_lower]
    # 이름 기반 추론
    for key, cat in CATEGORY_MAP.items():
        if key in slug_lower:
            return cat
    return "Other"


# ============================================================
# STEP 1: RWA 프로토콜 목록 수집
# ============================================================
def fetch_rwa_protocols():
    """DefiLlama /protocols → RWA 카테고리 필터링"""
    print("\n📥 Step 1: RWA 프로토콜 목록 수집...")
    data = api_get("/protocols")
    if not data:
        print("  ❌ 프로토콜 목록 가져오기 실패")
        return []

    rwa_protocols = []
    for p in data:
        category = (p.get("category") or "").lower()
        # DefiLlama의 RWA 관련 카테고리들
        if category in ["rwa", "rwa lending", "real world assets"]:
            rwa_protocols.append({
                "slug": p.get("slug", ""),
                "name": p.get("name", ""),
                "symbol": p.get("symbol", ""),
                "tvl": p.get("tvl", 0) or 0,
                "change_1d": p.get("change_1d", 0) or 0,
                "change_7d": p.get("change_7d", 0) or 0,
                "change_1m": p.get("change_1m", 0) or 0,
                "chains": p.get("chains", []),
                "defi_category": p.get("category", ""),
            })

    # TVL 기준 정렬
    rwa_protocols.sort(key=lambda x: x["tvl"], reverse=True)
    print(f"  → {len(rwa_protocols)}개 RWA 프로토콜 발견")
    return rwa_protocols


# ============================================================
# STEP 2: 프로토콜별 히스토리컬 TVL 수집
# ============================================================
def fetch_protocol_histories(protocols):
    """Top N 프로토콜의 히스토리컬 TVL 수집"""
    print(f"\n📥 Step 2: Top {TOP_N_PROTOCOLS} 프로토콜 히스토리 수집...")
    top = protocols[:TOP_N_PROTOCOLS]
    histories = {}

    for i, p in enumerate(top):
        slug = p["slug"]
        print(f"  [{i+1}/{len(top)}] {p['name']} ({slug})...")
        data = api_get(f"/protocol/{slug}")
        time.sleep(REQUEST_DELAY)

        if not data or "tvl" not in data:
            print(f"    ⚠ 데이터 없음, 스킵")
            continue

        tvl_series = data.get("tvl", [])
        if not tvl_series:
            continue

        daily = {}
        for point in tvl_series:
            date = ts_to_date(point["date"])
            daily[date] = point.get("totalLiquidityUSD", 0)

        histories[slug] = {
            "name": p["name"],
            "data": daily,
        }
        print(f"    → {len(daily)} data points")

    return histories


# ============================================================
# STEP 3: DeFi 전체 TVL 수집
# ============================================================
def fetch_defi_total_tvl():
    """DeFi 전체 히스토리컬 TVL"""
    print("\n📥 Step 3: DeFi 전체 TVL 수집...")
    data = api_get("/v2/historicalChainTvl")
    if not data:
        print("  ❌ 실패")
        return {}

    daily = {}
    for point in data:
        date = ts_to_date(point["date"])
        daily[date] = point.get("tvl", 0)

    print(f"  → {len(daily)} data points")
    return daily


# ============================================================
# STEP 4: 데이터 가공 및 CSV 저장
# ============================================================
def process_and_save(protocols, histories, defi_tvl):
    """수집된 데이터를 CSV 파일로 가공/저장"""
    print("\n💾 Step 4: CSV 저장...")

    # ---- (A) 프로토콜 순위 스냅샷 ----
    rows = []
    for i, p in enumerate(protocols[:30]):
        rows.append([
            i + 1,
            p["name"],
            p["slug"],
            p["symbol"],
            round(p["tvl"], 2),
            round(p["change_1d"], 2),
            round(p["change_7d"], 2),
            round(p["change_1m"], 2),
            get_category(p["slug"]),
            "|".join(p["chains"][:5]),
        ])
    save_csv("rwa_protocols.csv", [
        "rank", "name", "slug", "symbol", "tvl",
        "change_1d", "change_7d", "change_1m", "category", "chains"
    ], rows)

    # ---- (B) 체인별 분포 ----
    chain_tvl = defaultdict(float)
    for p in protocols:
        if p["tvl"] <= 0:
            continue
        chains = p["chains"]
        if not chains:
            chain_tvl["Unknown"] += p["tvl"]
            continue
        # TVL을 체인 수로 균등 분배 (근사치)
        # 더 정확하게 하려면 chainTvls 데이터 필요
        per_chain = p["tvl"] / len(chains)
        for c in chains:
            chain_tvl[c] += per_chain

    total_chain_tvl = sum(chain_tvl.values())
    chain_rows = sorted(chain_tvl.items(), key=lambda x: x[1], reverse=True)
    rows = []
    for chain, tvl in chain_rows[:20]:
        pct = (tvl / total_chain_tvl * 100) if total_chain_tvl > 0 else 0
        rows.append([chain, round(tvl, 2), round(pct, 2)])
    save_csv("rwa_chains.csv", ["chain", "tvl", "pct"], rows)

    # ---- (C) RWA 전체 TVL 히스토리 (일별 합산) ----
    all_dates_set = set()
    for h in histories.values():
        all_dates_set.update(h["data"].keys())

    all_dates = sorted(all_dates_set)

    # 전체 RWA TVL = 모든 프로토콜 합산
    rwa_daily = {}
    for date in all_dates:
        total = sum(h["data"].get(date, 0) for h in histories.values())
        rwa_daily[date] = total

    rows = []
    for date in all_dates:
        rwa_val = rwa_daily.get(date, 0)
        defi_val = defi_tvl.get(date, 0)
        ratio = (rwa_val / defi_val * 100) if defi_val > 0 else 0
        rows.append([date, round(rwa_val, 2), round(defi_val, 2), round(ratio, 4)])
    save_csv("rwa_tvl_history.csv", ["date", "rwa_tvl", "defi_tvl", "rwa_ratio_pct"], rows)

    # ---- (D) 카테고리별 TVL 히스토리 ----
    cat_daily = defaultdict(lambda: defaultdict(float))
    for slug, h in histories.items():
        cat = get_category(slug)
        for date, tvl in h["data"].items():
            cat_daily[date][cat] += tvl

    categories = ["Treasuries", "Private Credit", "Commodities", "Real Estate", "Other"]
    rows = []
    for date in all_dates:
        row = [date]
        for cat in categories:
            row.append(round(cat_daily[date].get(cat, 0), 2))
        rows.append(row)
    save_csv("rwa_category_history.csv", ["date"] + categories, rows)

    # ---- (E) Top N 프로토콜 히스토리 ----
    top_slugs = list(histories.keys())[:TOP_N_PROTOCOLS]
    header = ["date"] + [histories[s]["name"] for s in top_slugs]
    rows = []
    for date in all_dates:
        row = [date]
        for slug in top_slugs:
            row.append(round(histories[slug]["data"].get(date, 0), 2))
        rows.append(row)
    save_csv("rwa_top_protocols_history.csv", header, rows)

    # ---- (F) 메타데이터 ----
    meta = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_rwa_protocols": len(protocols),
        "top_protocols_tracked": TOP_N_PROTOCOLS,
        "total_rwa_tvl": round(sum(p["tvl"] for p in protocols), 2),
        "date_range": f"{all_dates[0]} ~ {all_dates[-1]}" if all_dates else "N/A",
    }
    meta_path = os.path.join(DATA_DIR, "meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    print(f"  ✅ meta.json")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("🐂 HerdVibe RWA 데이터 수집 시작")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    ensure_dir()

    # 1. RWA 프로토콜 목록
    protocols = fetch_rwa_protocols()
    if not protocols:
        print("\n❌ RWA 프로토콜을 찾을 수 없습니다. 종료.")
        return

    # 2. Top 프로토콜 히스토리
    histories = fetch_protocol_histories(protocols)

    # 3. DeFi 전체 TVL
    defi_tvl = fetch_defi_total_tvl()

    # 4. 가공 & 저장
    process_and_save(protocols, histories, defi_tvl)

    print("\n" + "=" * 60)
    print("✅ 데이터 수집 완료!")
    print("=" * 60)


if __name__ == "__main__":
    main()
