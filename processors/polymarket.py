import json
import math
from datetime import datetime, timedelta

# === ⚙️ 1. 基础配置 ===
TABLE_NAME = "polymarket_logs"  # 严格对应 SQL 表名
ARCHIVE_FOLDER = "polymarket"

# === 🛠️ 2. 数据清洗工具 (入库用) ===
def to_bj_time(utc_str):
    """把 UTC 时间强制转为北京时间 (ISO格式)"""
    if not utc_str: return None
    try:
        # 处理 Polymarket 的 Z 结尾时间
        dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
        return (dt + timedelta(hours=8)).isoformat()
    except: return None

def parse_num(val):
    """清洗数值：去掉逗号、$符号、百分号，转为 float"""
    if not val: return 0
    s = str(val).replace(',', '').replace('$', '').replace('%', '')
    try:
        return float(s)
    except:
        return 0

# === 📥 3. 入库算法 (Process) ===
def process(raw_data, path):
    processed_list = []
    
    # 自动识别引擎：从文件名判断是 sniper 还是 radar
    engine_type = "sniper" if "sniper" in path.lower() else "radar"
    
    for item in raw_data:
        # 构造符合 SQL 的字典
        entry = {
            "bj_time": to_bj_time(item.get('updatedAt')),
            "title": item.get('eventTitle'),
            "slug": item.get('slug'),
            "ticker": item.get('ticker'),
            "question": item.get('question'),
            "prices": str(item.get('prices')), # 存为文本
            "category": item.get('category', 'OTHER'),
            
            # 数值清洗
            "volume": parse_num(item.get('volume')),
            "liquidity": parse_num(item.get('liquidity')),
            "vol24h": parse_num(item.get('vol24h')),
            "day_change": parse_num(item.get('dayChange')),
            
            # 引擎与策略
            "engine": engine_type,
            "strategy_tags": item.get('strategy_tags', []), # 存为 JSONB
            
            # 完整备份 (防后悔药)
            "raw_json": item
        }
        processed_list.append(entry)
        
    return processed_list

# === 🧮 4. 动态审计评分 (出库计算用) ===
def calculate_score(item):
    """从数据库记录还原 V5 审计分"""
    vol24h = float(item.get('vol24h') or 0)
    # 兼容可能存在的不同字段名
    day_change = abs(float(item.get('dayChange') or item.get('day_change') or 0))
    
    # 基础公式：量 * (波动+1)
    score = vol24h * (day_change + 1)
    
    # 狙击加成 (基于 raw_json 或 title/question 字段)
    text = (str(item.get('title')) + " " + str(item.get('question'))).lower()
    snipers = ["gold", "bitcoin", "btc", "fed", "federal reserve", "xau"]
    # 剔除 warsh (他在 Radar 板块)
    if any(k in text for k in snipers) and "warsh" not in text:
        score *= 100
        
    # 策略加成 (从 JSONB 字段读)
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

# === 📤 5. 战报生成算法 (Get Hot Items) ===
def get_hot_items(supabase, table_name):
    # 1. 拉取过去 24 小时的数据
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    # select * 包含 raw_json 和 strategy_tags，足够计算 score
    res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
    if not res.data: return {}
    
    all_data = res.data
    
    # 2. 区分引擎池
    sniper_pool = [i for i in all_data if i.get('engine') == 'sniper']
    radar_pool = [i for i in all_data if i.get('engine') == 'radar']
    
    sector_matrix = {}

    # --- 🛡️ 核心：V5.1 防刷屏与补全逻辑 ---
    def anti_flood_filter(items):
        grouped = {}
        for i in items:
            s = i['slug']
            if s not in grouped: grouped[s] = []
            grouped[s].append(i)
        
        final = []
        for s, rows in grouped.items():
            # 先计算 score
            for r in rows: r['_temp_score'] = calculate_score(r)
            rows.sort(key=lambda x: x['_temp_score'], reverse=True)
            
            # 1. 优先提取：共识项 (>80%) 和 冲突项 (<15%)
            consensus = [r for r in rows if get_win_rate(r['prices']) > 80]
            conflict = [r for r in rows if get_win_rate(r['prices']) < 15]
            
            picks = []
            if consensus: picks.append(consensus[0])
            if conflict: picks.append(conflict[0])
            
            # 2. 兜底：如果没选中，取带头大哥
            if not picks: picks.append(rows[0])
            
            # 3. 🔥 二号位补全 (V5.1) 🔥
            # 如果只选了1个，且老二很强(>老大的20%)，把它也捞回来，防止漏掉势均力敌的对手
            if len(picks) < 2 and len(rows) > 1:
                top_item = picks[0]
                for candidate in rows:
                    if candidate['question'] == top_item['question']: continue
                    if candidate['_temp_score'] > (top_item['_temp_score'] * 0.2):
                        picks.append(candidate)
                        break
            
            final.extend(picks[:2]) # 最终每个事件限 2 条
        return final

    # A. 狙击区 (Sniper) - 全量展示
    if sniper_pool:
        refined = anti_flood_filter(sniper_pool)
        refined.sort(key=lambda x: x['_temp_score'], reverse=True)
        
        display_list = []
        for i in refined:
            display_list.append({
                "score": i['_temp_score'],
                "user_name": f"SNIPER | {get_win_rate(i['prices'])}%",
                "full_text": f"{i['question']} (Vol: ${int(i['vol24h']):,})",
                # 🔥 修正：使用通用 url 字段，不再用 tweet_url
                "url": f"https://polymarket.com/event/{i['slug']}"
            })
        sector_matrix["🎯 SNIPER (核心监控)"] = display_list

    # B. 雷达区 (Radar) - 比例配额
    SECTORS = ["Politics", "Geopolitics", "Science", "Tech", "Finance", "Crypto", "Economy"]
    # 简写映射表
    MAP = {'POLITICS': 'Politics', 'GEOPOLITICS': 'Geopolitics', 'TECH': 'Tech', 'FINANCE': 'Finance', 'CRYPTO': 'Crypto'}
    
    if radar_pool:
        for s in SECTORS:
            # 过滤当前板块的数据
            pool = [i for i in radar_pool if MAP.get(i.get('category'), 'Other') == s or i.get('category') == s.upper()]
            if not pool: continue
            
            refined = anti_flood_filter(pool)
            refined.sort(key=lambda x: x['_temp_score'], reverse=True)
            
            # 动态配额：占比 * 30，最少 3 条
            quota = max(3, math.ceil((len(pool) / len(radar_pool)) * 30))
            
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
