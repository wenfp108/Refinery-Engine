import json
import math
from datetime import datetime, timedelta

# === ⚙️ 1. 基础配置 ===
TABLE_NAME = "twitter_logs"
ARCHIVE_FOLDER = "twitter"

SECTORS = ["Politics", "Geopolitics", "Science", "Tech", "Finance", "Crypto", "Economy"]
TARGET_TOTAL_QUOTA = 30  # 基准总配额

# === 🛠️ 2. 数据清洗 (入库) ===
def to_iso_bj(date_str):
    try:
        utc_dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S +0000 %Y')
        return (utc_dt + timedelta(hours=8)).isoformat()
    except:
        return datetime.now().isoformat()

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
            # 🔥 [Pro修正] 字段统一为 url，方便引擎调用
            "url": i.get('tweetUrl'), 
            "tags": i.get('tags', []),
            
            # 基础数据
            "likes": metrics.get('likes', 0),
            "retweets": metrics.get('retweets', 0),
            "replies": metrics.get('replies', 0),
            "quotes": metrics.get('quotes', 0),
            "bookmarks": metrics.get('bookmarks', 0),
            "views": metrics.get('views', 0),
            
            # 增长数据 (用于计算爆发力)
            "growth_views": growth.get('views', 0),
            "growth_likes": growth.get('likes', 0),
            "growth_retweets": growth.get('retweets', 0),
            "growth_replies": growth.get('replies', 0),
            
            "raw_json": i 
        }
        refined_results.append(row)
    return refined_results

# === 🧮 3. 核心打分公式 (Python版) ===
def calculate_twitter_score(item):
    """
    复刻原 SQL 逻辑：
    (基础互动加权 + 增长爆发力加权) * (1 + 标签协同系数)
    """
    base_interaction = (
        item.get('retweets', 0) * 8 + 
        item.get('quotes', 0) * 12 + 
        item.get('replies', 0) * 5 + 
        item.get('bookmarks', 0) * 10
    )
    
    growth_momentum = (
        item.get('growth_likes', 0) * 15 + 
        item.get('growth_retweets', 0) * 25 + 
        item.get('growth_replies', 0) * 10
    )
    
    # 标签越多，跨界影响力越大，系数越高
    synergy_boost = 1 + (len(item.get('tags', [])) * 0.3)
    
    return (base_interaction + growth_momentum) * synergy_boost

# === 📤 4. 战报生成 (动态配额版) ===
def get_hot_items(supabase, table_name):
    # 1. 一次性拉取过去 24 小时全量数据 (内存计算比 7 次 SQL 快且准)
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        # 这里不需要 order，拉回来 Python 算
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_tweets = res.data if res.data else []
    except Exception as e:
        print(f"⚠️ Twitter 数据拉取失败: {e}")
        return {}

    if not all_tweets: return {}

    # 2. 预计算所有推文的分数
    for t in all_tweets:
        t['_score'] = calculate_twitter_score(t)

    # 3. 计算板块密度 (一人多签逻辑)
    total_unique_tweets = len(all_tweets)
    sector_pools = {s: [] for s in SECTORS}
    
    for t in all_tweets:
        tags = t.get('tags', [])
        # 如果一条推文有 Tech 和 Crypto，它会同时进入两个池子
        for tag in tags:
            if tag in sector_pools:
                sector_pools[tag].append(t)

    # 4. 生成最终矩阵
    intelligence_matrix = {}
    
    for sector, pool in sector_pools.items():
        if not pool: continue
        
        # 按分数硬核排序
        pool.sort(key=lambda x: x['_score'], reverse=True)
        
        # 🔥 动态配额公式 🔥
        # (该板块推文数 / 总唯一推文数) * 30
        # 即使总和超过 30 也没关系，这代表推文的跨界热度
        quota = max(3, math.ceil((len(pool) / total_unique_tweets) * TARGET_TOTAL_QUOTA))
        
        # 提取展示项
        display_items = []
        for t in pool[:quota]:
            display_items.append({
                "score": int(t['_score']),
                "user_name": t['user_name'],
                "full_text": t['full_text'],
                "tweet_url": t['url'] # 对应 refinery.py 的通用字段
            })
        
        intelligence_matrix[sector] = display_items

    return intelligence_matrix
