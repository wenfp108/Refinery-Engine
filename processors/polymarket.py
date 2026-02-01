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

# 🔥 修复 f-string 报错
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

    # 🔥 1. 快照去重：只留最新时间戳
    def deduplicate_snapshots(items):
        latest_map = {}
        for item in items:
            unique_key = f"{item['slug']}_{item['question']}"
            if unique_key not in latest_map:
                latest_map[unique_key] = item
            else:
                if item.get('bj_time', '0') > latest_map[unique_key].get('bj_time', '0'):
                    latest_map[unique_key] = item
        return list(latest_map.values())

    clean_data = deduplicate_snapshots(all_data)
    
    sniper_pool = [i for i in clean_data if i.get('engine') == 'sniper']
    radar_pool = [i for i in clean_data if i.get('engine') == 'radar']
    sector_matrix = {}
    global_seen_slugs = set()

    def anti_flood_filter(items):
        grouped = {}
        for i in items:
            s = i['slug']
            if s not in grouped: grouped[s] = []
            grouped[s].append(i)
        final = []
        for s, rows in grouped.items():
            for r in rows: r['_temp_score'] = calculate_score(r)
            rows.sort(key=lambda x: x['_temp_score'], reverse=True)
            final.extend(rows[:2])
        return final

    # 🔥 2. 构建 8 列宽表
    def build_markdown(items):
        header = "| 信号 | 标题 | 问题 | Prices | Vol | Liq | 24h | Tags |\n| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |"
        rows = []
        for i in items:
            signal = fmt_k(i['_temp_score'])
            title = str(i.get('title', '-'))[:20].replace('|', '') 
            q_text = str(i.get('question', '-'))[:40].replace('|', '') + "..."
            question = f"[{q_text}](https://polymarket.com/event/{i['slug']})"
            prices = get_win_rate_str(i['prices'])
            vol = fmt_k(i.get('volume', 0), '$')
            liq = fmt_k(i.get('liquidity', 0), '$')
            v24 = fmt_k(i.get('vol24h', 0), '$')
            tags = ", ".join(i.get('strategy_tags', []))[:15]

            row = f"| **{signal}** | {title} | {question} | {prices} | {vol} | {liq} | {v24} | {tags} |"
            rows.append(row)
            global_seen_slugs.add(i['slug'])
        return {"header": header, "rows": rows}

    if sniper_pool:
        refined = anti_flood_filter(sniper_pool)
        refined.sort(key=lambda x: x['_temp_score'], reverse=True)
        sector_matrix["🎯 SNIPER (核心监控)"] = build_markdown(refined)

    # 🔥 3. 顺序：政治压轴
    SECTORS_LIST = [
        "Geopolitics", "Science", "Climate-Science", "Tech", 
        "Finance", "Crypto", "Economy", "Politics"
    ]
    
    MAP = {
        'POLITICS': 'Politics', 'GEOPOLITICS': 'Geopolitics', 'TECH': 'Tech', 
        'FINANCE': 'Finance', 'CRYPTO': 'Crypto', 'SCIENCE': 'Science', 
        'ECONOMY': 'Economy', 'BUSINESS': 'Economy',
        'CLIMATE': 'Climate-Science', 'GLOBAL WARMING': 'Climate-Science', 'ENVIRONMENT': 'Climate-Science'
    }

    if radar_pool:
        for s in SECTORS_LIST:
            pool = [
                i for i in radar_pool 
                if (MAP.get(i.get('category'), 'Other') == s or i.get('category') == s.upper())
                and i['slug'] not in global_seen_slugs
            ]
            if not pool: continue
            refined = anti_flood_filter(pool)
            refined.sort(key=lambda x: x['_temp_score'], reverse=True)
            quota = max(3, math.ceil((len(pool) / len(radar_pool)) * RADAR_TARGET_TOTAL))
            sector_matrix[s] = build_markdown(refined[:quota])

    return sector_matrix
