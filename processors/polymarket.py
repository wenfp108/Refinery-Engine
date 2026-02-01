import json
import math
from datetime import datetime, timedelta

TABLE_NAME = "polymarket_logs"
RADAR_TARGET_TOTAL = 50  

# 🎨 美化工具
def fmt_k(num, prefix=""):
    if not num: return "-"
    try: n = float(num)
    except: return "-"
    if n >= 1_000_000_000_000: return f"{prefix}{n/1_000_000_000_000:.1f}T"
    if n >= 1_000_000_000: return f"{prefix}{n/1_000_000_000:.1f}B"
    if n >= 1_000_000: return f"{prefix}{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{prefix}{n/1_000:.1f}K"
    return f"{prefix}{int(n)}"

def to_bj_time(utc_str):
    if not utc_str: return None
    try:
        dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
        return (dt + timedelta(hours=8)).isoformat()
    except: return None

def parse_num(val):
    if not val: return 0
    s = str(val).replace(',', '').replace('$', '').replace('%', '')
    try: return float(s)
    except: return 0

def process(raw_data, path):
    processed_list = []
    engine_type = "sniper" if "sniper" in path.lower() else "radar"
    if isinstance(raw_data, dict) and "items" in raw_data: items = raw_data["items"]
    elif isinstance(raw_data, list): items = raw_data
    else: items = [raw_data]
    
    # 强制刷新时间戳
    force_now_time = (datetime.utcnow() + timedelta(hours=8)).isoformat()
    
    for item in items:
        entry = {
            "bj_time": force_now_time,
            "title": item.get('eventTitle'),
            "slug": item.get('slug'),
            "ticker": item.get('ticker'),
            "question": item.get('question'),
            "prices": str(item.get('prices')),
            "category": item.get('category', 'OTHER'),
            "volume": parse_num(item.get('volume')),
            "liquidity": parse_num(item.get('liquidity')),
            "vol24h": parse_num(item.get('vol24h')),
            "day_change": parse_num(item.get('dayChange')),
            "engine": engine_type,
            "strategy_tags": item.get('strategy_tags', []),
            "raw_json": item
        }
        processed_list.append(entry)
    return processed_list

def calculate_score(item):
    vol24h = float(item.get('vol24h') or 0)
    day_change = abs(float(item.get('dayChange') or item.get('day_change') or 0))
    score = vol24h * (day_change + 1)
    text = (str(item.get('title')) + " " + str(item.get('question'))).lower()
    snipers = ["gold", "bitcoin", "btc", "fed", "federal reserve", "xau"]
    if any(k in text for k in snipers) and "warsh" not in text: score *= 100
    tags = item.get('strategy_tags') or []
    if 'TAIL_RISK' in tags: score *= 50
    return score

def get_win_rate_str(price_str):
    try:
        if "Yes:" in price_str: 
            val = float(price_str.split('Yes:')[1].split('%')[0])
            return f"Yes {val:.0f}%"
        if "Up:" in price_str: 
            val = float(price_str.split('Up:')[1].split('%')[0])
            return f"Up {val:.0f}%"
        if "{" in price_str:
            clean_json = price_str.replace("'", '"')
            val = float(json.loads(clean_json)) * 100
            return f"{val:.0f}%"
    except: pass
    return str(price_str)[:15]

def get_hot_items(supabase, table_name):
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_data = res.data if res.data else []
    except Exception as e: return {}
    if not all_data: return {}

    # 🔥🔥 核心去重：只保留每个问题的最新快照 🔥🔥
    def deduplicate_snapshots(items):
        latest_map = {}
        for item in items:
            # 唯一标识：slug (市场) + question (具体问题)
            # 这样就能区分 "Bitcoin > 80k" 和 "Bitcoin > 90k"
            unique_key = f"{item['slug']}_{item['question']}"
            
            if unique_key not in latest_map:
                latest_map[unique_key] = item
            else:
                #
