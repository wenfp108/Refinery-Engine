import json
import math
from datetime import datetime, timedelta

TABLE_NAME = "twitter_logs"
SECTORS = ["Politics", "Geopolitics", "Science", "Tech", "Finance", "Crypto", "Economy"]
TARGET_TOTAL_QUOTA = 30 

def fmt_k(num):
    if not num: return "-"
    try: n = float(num)
    except: return "-"
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(int(n))

def to_iso_bj(date_str):
    try:
        utc_dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S +0000 %Y')
        return (utc_dt + timedelta(hours=8)).isoformat()
    except: return datetime.now().isoformat()

def process(raw_data, path):
    items = raw_data if isinstance(raw_data, list) else [raw_data]
    refined_results = []
    for i in items:
        user = i.get('user', {})
        metrics = i.get('metrics', {})
        growth = i.get('growth', {})
        row = {
            "bj_time": to_iso_bj(i.get('createdAt')),
            "user_name": user.get('name'),
            "screen_name": user.get('screenName'),
            "followers_count": user.get('followersCount'),
            "full_text": i.get('fullText'),
            "url": i.get('tweetUrl'), 
            "tags": i.get('tags', []),
            "likes": metrics.get('likes', 0),
            "retweets": metrics.get('retweets', 0),
            "replies": metrics.get('replies', 0),
            "quotes": metrics.get('quotes', 0),
            "bookmarks": metrics.get('bookmarks', 0),
            "views": metrics.get('views', 0),
            "growth_views": growth.get('views', 0),
            "growth_likes": growth.get('likes', 0),
            "growth_retweets": growth.get('retweets', 0),
            "growth_replies": growth.get('replies', 0),
            "raw_json": i 
        }
        refined_results.append(row)
    return refined_results

def calculate_twitter_score(item):
    base = (item.get('retweets', 0)*8 + item.get('quotes', 0)*12 + item.get('replies', 0)*5 + item.get('bookmarks', 0)*10)
    growth = (item.get('growth_likes', 0)*15 + item.get('growth_retweets', 0)*25 + item.get('growth_replies', 0)*10)
    synergy = 1 + (len(item.get('tags', [])) * 0.3)
    return (base + growth) * synergy

def get_hot_items(supabase, table_name):
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_tweets = res.data if res.data else []
    except Exception as e: return {}

    if not all_tweets: return {}

    for t in all_tweets: t['_score'] = calculate_twitter_score(t)

    unique_map = {}
    for t in all_tweets:
        key = t.get('url') or (t.get('user_name'), t.get('full_text'))
        if key not in unique_map or t['_score'] > unique_map[key]['_score']:
            unique_map[key] = t
    deduplicated = list(unique_map.values())
    total = len(deduplicated)

    sector_pools = {s: [] for s in SECTORS}
    for t in deduplicated:
        tags = t.get('tags', [])
        for sector in SECTORS:
            if sector in tags:
                sector_pools[sector].append(t)
                break 

    intelligence_matrix = {}
    for sector, pool in sector_pools.items():
        if not pool: continue
        pool.sort(key=lambda x: x['_score'], reverse=True)
        quota = max(3, math.ceil((len(pool) / total) * TARGET_TOTAL_QUOTA))
        
        header = "| 信号 | 浏览量 | 博主 | 摘要 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- |"
        rows = []
        for t in pool[:quota]:
            score = fmt_k(t['_score'])
            views = fmt_k(t.get('views', 0))
            user = t['user_name']
            text = t['full_text'].replace('\n', ' ')[:60] + "..."
            url = t['url']
            rows.append(f"| **{score}** | 👁️ {views} | {user} | {text} | [🔗]({url}) |")
        
        intelligence_matrix[sector] = {"header": header, "rows": rows}

    return intelligence_matrix
