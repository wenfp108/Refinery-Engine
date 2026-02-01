import json
import math
from datetime import datetime, timedelta

# === ⚙️ 1. 基础配置 (控制台) ===
TABLE_NAME = "polymarket_logs"
ARCHIVE_FOLDER = "polymarket"

# 🔥 [FOMO 开关] Radar 区的总配额锚点
# 建议：30 = 精英简报 (只看头部)；50 = 深度情报 (包含中腰部)；80 = 数据终端 (几乎全量)
RADAR_TARGET_TOTAL = 50  

# === 🛠️ 2. 数据清洗工具 (入库用) ===
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

# === 📥 3. 入库算法 (Process) ===
def process(raw_data, path):
    processed_list = []
    engine_type = "sniper" if "sniper" in path.lower() else "radar"
    
    for item in raw_data:
        entry = {
            "bj_time": to_bj_time(item.get('updatedAt')),
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

# === 🧮 4. 动态审计评分 ===
def calculate_score(item):
    vol24h = float(item.get('vol24h') or 0)
    day_change = abs(float(item.get('dayChange') or item.get('day_change') or 0))
    score = vol24h * (day_change + 1)
    
    text = (str(item.get('title')) + " " + str(item.get('question'))).lower()
    snipers = ["gold", "bitcoin", "btc", "fed", "federal reserve", "xau"]
    if any(k in text for k in snipers) and "warsh" not in text:
        score *= 100
        
    tags = item.get('strategy_tags') or []
    if 'TAIL_RISK' in tags: score *= 50
    if 'HIGH_CERTAINTY' in tags: score *= 30
    return score

def get_win_rate(price_str):
    try:
        if "Yes: " in price_str: return float(price_str.split("Yes: ")[1].split("%")[0])
        if "Up: " in price_str: return float(price_str.split("Up: ")[1].split("%")[0])
    except: pass
    return 50.0

# === 📤 5. 战报生成 (Get Hot Items) ===
def get_hot_items(supabase, table_name):
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
    if not res.data: return {}
    
    all_data = res.data
    sniper_pool = [i for i in all_data if i.get('engine') == 'sniper']
    radar_pool = [i for i in all_data if i.get('engine') == 'radar']
    
    sector_matrix = {}

    # --- V5.1 防刷屏逻辑 ---
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
            
            consensus = [r for r in rows if get_win_rate(r['prices']) > 80]
            conflict = [r for r in rows if get_win_rate(r['prices']) < 15]
            
            picks = []
            if consensus: picks.append(consensus[0])
            if conflict: picks.append(conflict[0])
            if not picks: picks.append(rows[0])
            
            # 二号位补全
            if len(picks) < 2 and len(rows) > 1:
                top_item = picks[0]
                for candidate in rows:
                    if candidate['question'] == top_item['question']: continue
                    if candidate['_temp_score'] > (top_item['_temp_score'] * 0.2):
                        picks.append(candidate)
                        break
            final.extend(picks[:2])
        return final

    # A. 狙击区 (全量)
    if sniper_pool:
        refined = anti_flood_filter(sniper_pool)
        refined.sort(key=lambda x: x['_temp_score'], reverse=True)
        display_list = []
        for i in refined:
            display_list.append({
                "score": i['_temp_score'],
                "user_name": f"SNIPER | {get_win_rate(i['prices'])}%",
                "full_text": f"{i['question']} (Vol: ${int(i['vol24h']):,})",
                "url": f"https://polymarket.com/event/{i['slug']}"
            })
        sector_matrix["🎯 SNIPER (核心监控)"] = display_list

    # B. 雷达区 (动态配额)
    SECTORS = ["Politics", "Geopolitics", "Science", "Tech", "Finance", "Crypto", "Economy"]
    MAP = {'POLITICS': 'Politics', 'GEOPOLITICS': 'Geopolitics', 'TECH': 'Tech', 'FINANCE': 'Finance', 'CRYPTO': 'Crypto'}
    
    if radar_pool:
        for s in SECTORS:
            pool = [i for i in radar_pool if MAP.get(i.get('category'), 'Other') == s or i.get('category') == s.upper()]
            if not pool: continue
            
            refined = anti_flood_filter(pool)
            refined.sort(key=lambda x: x['_temp_score'], reverse=True)
            
            # 🔥 动态配额的核心修改：使用全局变量 RADAR_TARGET_TOTAL 🔥
            quota = max(3, math.ceil((len(pool) / len(radar_pool)) * RADAR_TARGET_TOTAL))
            
            display_list = []
            for i in refined[:quota]:
                display_list.append({
                    "score": i['_temp_score'],
                    "user_name": f"{s} | {get_win_rate(i['prices'])}%",
                    "full_text": f"{i['title']} -> {i['question']}",
                    "url": f"https://polymarket.com/event/{i['slug']}"
                })
            sector_matrix[s] = display_list

    return sector_matrix
