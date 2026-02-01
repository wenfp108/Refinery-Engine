import json
import math
from datetime import datetime, timedelta

TABLE_NAME = "twitter_logs"
TARGET_TOTAL_QUOTA = 30  # 严格筛选 Top 30

# === 🛑 1. 政治噪音词 (出现即降权，除非有豁免) ===
POLITICAL_NOISE = [
    "woke", "maga", "democrat", "republican", "leftist", "right wing", "liberal", "conservative",
    "fascist", "communist", "socialist", "pronouns", "dei", "border crisis", "illegal",
    "trump", "biden", "harris", "vance", "pelosi", "schumer", "election", "ballot",
    "scandal", "epstein", "pedophile", "traitor", "shame", "disgrace", "culture war"
]

# === 🔰 2. 宏观豁免词 (保护正经事) ===
# 即使有 Trump，如果有这些词，也视为高价值情报
MACRO_IMMUNITY = [
    "fed", "federal reserve", "powell", "fomc", "rate", "interest", "cut", "hike",
    "tariff", "trade war", "sanction", "export", "import", "duty",
    "china", "taiwan", "russia", "ukraine", "israel", "iran", "war", "military",
    "stimulus", "debt", "deficit", "budget", "tax", "treasury", "bond", "yield",
    "bitcoin", "btc", "crypto", "ban", "regulation", "sec", "gensler", "etf",
    "executive order", "veto", "sign", "bill", "act", "law", "legislation",
    "nominate", "nominee", "appoint", "confirm", "supreme court"
]

# === 🧠 3. 话题识别库 (用于打标签 + 核心加分) ===
TOPIC_KEYWORDS = {
    "Crypto": [
        "bitcoin", "btc", "eth", "solana", "defi", "nft", "stablecoin", "etf", "blackrock",
        "airdrop", "staking", "binance", "coinbase", "satoshi", "vitalik", "wallet"
    ],
    "AI/Tech": [
        "ai", "llm", "transformer", "inference", "training", "gpt", "claude", "gemini",
        "nvidia", "gpu", "h100", "cuda", "tsmc", "asml", "chip", "semiconductor",
        "spacex", "tesla", "fsd", "optimus", "python", "code", "github", "arxiv"
    ],
    "Science": [
        "nature", "science", "arxiv", "paper", "nasa", "jwst", "supernova", "quantum",
        "superconductor", "fusion", "crispr", "cancer", "alzheimer", "longevity"
    ],
    "Macro": [
        "sp500", "nasdaq", "bond", "yield", "gold", "oil", "revenue", "earnings",
        "fed", "rate", "cpi", "inflation", "gdp", "recession", "unemployment", "debt"
    ],
    "Geo": [
        "ukraine", "russia", "israel", "iran", "china", "taiwan", "war", "military", "nuclear"
    ]
}

# === 🛡️ 4. VIP 白名单 (保送机制) ===
VIP_AUTHORS = [
    "Karpathy", "Yann LeCun", "Vitalik", "Paul Graham", "Naval", 
    "Eric Topol", "Huberman", "Lex Fridman", "Sam Altman", "Kobeissi Letter",
    "Michael Saylor", "Balaji"
]

def fmt_k(num):
    if not num: return "0"
    try: n = float(num)
    except: return "0"
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
            "bookmarks": metrics.get('bookmarks', 0),
            "raw_json": i 
        }
        refined_results.append(row)
    return refined_results

# 🔥 核心：上帝权重算法 🔥
def calculate_score_and_tag(item):
    text = (item.get('full_text') or "").lower()
    user = (item.get('user_name') or "")
    
    # 1. 基础热度 (书签权重最高，代表深度价值)
    metrics = item.get('raw_json', {}).get('metrics', {})
    likes = metrics.get('likes', 0)
    retweets = metrics.get('retweets', 0)
    bookmarks = metrics.get('bookmarks', 0)
    base_score = (retweets * 5) + (bookmarks * 10) + likes
    
    # 2. 话题识别 & 加权
    detected_topic = "General"
    is_hardcore = False
    
    for topic, keywords in TOPIC_KEYWORDS.items():
        for k in keywords:
            if k in text:
                detected_topic = topic
                is_hardcore = True
                break
        if is_hardcore: break
    
    # 硬核话题加分 (Tech, Crypto, Science, Macro)
    if is_hardcore:
        base_score += 2000 # 只要沾边硬核，起步分拉高
        base_score *= 1.5  # 倍率加成
        
    # 3. 政治排毒 (逻辑：有噪音且无豁免 -> 降权)
    has_noise = False
    for noise in POLITICAL_NOISE:
        if noise in text:
            has_noise = True
            break
            
    if has_noise:
        is_immune = False
        for safe in MACRO_IMMUNITY:
            if safe in text:
                is_immune = True
                break
        if not is_immune:
            base_score *= 0.1 # 降权打击
            detected_topic = "Politics" # 强制标记为政治
            
    # 4. VIP 加成
    for vip in VIP_AUTHORS:
        if vip.lower() in user.lower():
            base_score += 5000
            break
            
    return base_score, detected_topic

def get_hot_items(supabase, table_name):
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_tweets = res.data if res.data else []
    except Exception as e: return {}

    if not all_tweets: return {}

    # 1. URL 去重
    unique_map = {}
    for t in all_tweets:
        key = t.get('url') or (t.get('user_name'), t.get('full_text'))
        if key not in unique_map:
            unique_map[key] = t
    tweets = list(unique_map.values())

    # 2. 算分 & 打标
    scored_tweets = []
    for t in tweets:
        score, topic = calculate_score_and_tag(t)
        t['_score'] = score
        t['_topic'] = topic
        scored_tweets.append(t)
        
    # 3. 全局排序
    scored_tweets.sort(key=lambda x: x['_score'], reverse=True)
    
    # 4. 🛡️ 熔断机制 (Diversity Breaker) 🛡️
    # 应对大数据量的关键：防止同一个人霸榜
    final_list = []
    author_counts = {}
    
    for t in scored_tweets:
        if len(final_list) >= TARGET_TOTAL_QUOTA:
            break
            
        author = t['user_name']
        # 限制每个博主最多 3 条
        if author_counts.get(author, 0) >= 3:
            continue 
            
        final_list.append(t)
        author_counts[author] = author_counts.get(author, 0) + 1
        
    # 5. 生成单张大表
    header = "| 信号 | 🏷️ 标签 | 热度 | 博主 | 摘要 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- | :--- |"
    rows = []
    
    for t in final_list:
        score_display = fmt_k(t['_score'])
        topic_display = f"`{t['_topic']}`" # 代码块样式
        
        heat = f"❤️ {fmt_k(t.get('likes',0))}<br>🔁 {fmt_k(t.get('retweets',0))}" 
        user = t['user_name']
        text = t['full_text'].replace('\n', ' ')[:70] + "..."
        url = t['url']
        
        rows.append(f"| **{score_display}** | {topic_display} | {heat} | {user} | {text} | [🔗]({url}) |")

    return {"🏆 全域精选 (Top 30)": {"header": header, "rows": rows}}
