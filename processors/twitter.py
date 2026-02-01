import json
import math
from datetime import datetime, timedelta

# === ⚙️ 配置区 (已更新) ===

TABLE_NAME = "twitter_logs"
TARGET_TOTAL_QUOTA = 30  # 🌟 最终只选出全网最好的 30 条

# === 🛑 1. 政治/垃圾噪音词 (已针对新板块优化) ===
# 既然 "Politics" 现在是正经板块，我们只杀无意义的情绪宣泄词
NOISE_KEYWORDS = [
    "woke", "libtard", "magatard", "shame", "disgrace", "traitor", 
    "pedophile", "epstein", "pronouns", "culture war", "scandal",
    "destroy", "lies", "liar", "clown", "hypocrite", "idiot"
]

# === 🔰 2. 宏观豁免词 (保护长文不被误杀) ===
MACRO_IMMUNITY = [
    "fed", "federal reserve", "powell", "fomc", "rate", "interest", "cut", "hike",
    "tariff", "trade war", "sanction", "export", "import", "duty",
    "china", "taiwan", "russia", "ukraine", "israel", "iran", "war", "military",
    "stimulus", "debt", "deficit", "budget", "tax", "treasury", "bond", "yield",
    "bitcoin", "btc", "crypto", "ban", "regulation", "sec", "etf",
    "executive order", "veto", "sign", "bill", "act", "law", "legislation",
    "nominate", "nominee", "appoint", "confirm", "supreme court", "ruling"
]

# === 🧠 3. 精准话题词库 (7大板块 - 权重竞价模式) ===
# 包含：Tech, Politics, Finance, Economy, Geo, Science, Crypto
TOPIC_RULES = {
    "Tech": [ # 科技：AI, 芯片, 编程, 硬科技
        "llm", "genai", "gpt-5", "gpt-4", "claude", "gemini", "llama", "deepseek", "anthropic", "openai",
        "nvidia", "nvda", "h100", "blackwell", "cuda", "gpu", "semiconductor", "tsmc", "asml", "wafer",
        "spacex", "starship", "falcon", "tesla", "tsla", "fsd", "robot", "optimus", "figure ai",
        "python", "rust", "github", "huggingface", "open source", "coding"
    ],
    "Politics": [ # 政治：选举, 立法, 机构 (正经讨论)
        "white house", "biden", "trump", "harris", "vance", "congress", "senate", "house of rep",
        "supreme court", "scotus", "legislation", "bill", "veto", "executive order", "amendment",
        "election", "poll", "voter", "ballot", "campaign", "republican", "democrat", "gop", "dnc"
    ],
    "Finance": [ # 金融：二级市场, 投行, 财报 (Micro)
        "sp500", "nasdaq", "spx", "ndx", "dow jones", "russell 2000", "vix",
        "stock", "equity", "earnings", "revenue", "margin", "guidance", "buyback", "dividend",
        "goldman", "jpmorgan", "morgan stanley", "bloomberg", "blackrock", "citadel",
        "ipo", "merger", "acquisition", "short seller", "long position", "call option", "put option"
    ],
    "Economy": [ # 经济：宏观, 央行, 周期 (Macro)
        "fomc", "federal reserve", "jerome powell", "fed funds", "interest rate", "hike", "cut",
        "cpi", "ppi", "pce", "inflation", "deflation", "stagflation", "recession", "soft landing",
        "gdp", "unemployment", "jobless", "non-farm", "payroll", "labor market",
        "treasury", "bond yield", "10y", "2y", "curve inversion", "debt ceiling", "deficit"
    ],
    "Geo": [ # 地缘：战争, 外交, 制裁
        "ukraine", "russia", "putin", "zelensky", "kursk", "kyiv",
        "israel", "gaza", "hamas", "iran", "tehran", "red sea", "houthi", "hezbollah",
        "china", "xi jinping", "taiwan", "south china sea", "pla", "ccp",
        "nato", "pentagon", "nuclear", "weapon", "sanction", "trade war", "tariff"
    ],
    "Science": [ # 科学：学术, 能源, 生物, 航天
        "nature journal", "science magazine", "arxiv", "peer review", "preprint",
        "nasa", "esa", "jwst", "supernova", "exoplanet", "quantum", "fusion energy", "lk-99",
        "crispr", "mrna", "cancer", "alzheimer", "longevity", "biology", "physics", "chemistry"
    ],
    "Crypto": [ # 加密：Web3, 币, 链
        "bitcoin", "btc", "ethereum", "eth", "solana", "defi", "stablecoin", "usdc", "usdt",
        "etf flow", "blackrock", "coinbase", "binance", "satoshi", "vitalik", "memecoin",
        "wallet", "private key", "smart contract", "layer2", "zk-rollup", "airdrop"
    ]
}

# === 🛡️ 4. VIP 白名单 (基础分加成) ===
# 基于你提供的列表整合，涵盖所有板块领袖
VIP_AUTHORS = [
    # Tech / AI
    "Karpathy", "Yann LeCun", "Paul Graham", "Sam Altman", "François Chollet", 
    "Rowan Cheung", "Naval", "Palmer Luckey", "Anduril", "Elon Musk",
    
    # Finance / Macro / Economy
    "Nick Timiraos", "Ray Dalio", "Mohamed A. El-Erian", "Kobeissi Letter", 
    "Walter Bloomberg", "Zerohedge", "Lyn Alden", "MacroAlf", "Goldman Sachs",
    "Peter Schiff", "Michael Saylor", "Nassim Nicholas Taleb", "CME Group",
    "Fitch Ratings", "IMF", "Unusual Whales", "The Economist", "WSJ Central Banks",
    
    # Geo / Politics / Science
    "Ian Bremmer", "Eric Topol", "Vitalik", "SentDefender", "Visegrád 24",
    "Spectator Index", "Disclose.tv", "Defense News", "Council on Foreign Relations"
]

# === ⚙️ 核心逻辑函数 (完全保持原样) ===

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
        # 🗑️ 垃圾过滤：杀掉 "Yes..." 这种水贴
        text = i.get('fullText', '')
        # 如果正文太短(<10字)且不包含链接，直接丢弃
        if len(text) < 10 and 'http' not in text:
            continue

        user = i.get('user', {})
        metrics = i.get('metrics', {})
        row = {
            "bj_time": to_iso_bj(i.get('createdAt')),
            "user_name": user.get('name'),
            "screen_name": user.get('screenName'),
            "followers_count": user.get('followersCount'),
            "full_text": text,
            "url": i.get('tweetUrl'), 
            "tags": i.get('tags', []),
            "likes": metrics.get('likes', 0),
            "retweets": metrics.get('retweets', 0),
            "bookmarks": metrics.get('bookmarks', 0),
            "raw_json": i 
        }
        refined_results.append(row)
    return refined_results

# 🔥 核心：上帝权重算法 4.0 (Final Logic) 🔥
def calculate_score_and_tag(item):
    text = (item.get('full_text') or "").lower()
    user = (item.get('user_name') or "")
    
    # 1. 基础热度 (书签 x10, 转推 x5, 点赞 x1)
    metrics = item.get('raw_json', {}).get('metrics', {})
    base_score = (metrics.get('retweets', 0) * 5) + \
                 (metrics.get('bookmarks', 0) * 10) + \
                 metrics.get('likes', 0)
    
    # 2. 话题竞价 (Strict Tagging)
    detected_topic = "General"
    max_keyword_len = 0 # 匹配到的关键词越长，置信度越高
    
    for topic, keywords in TOPIC_RULES.items():
        for k in keywords:
            if k in text:
                if len(k) > max_keyword_len:
                    detected_topic = topic
                    max_keyword_len = len(k)
    
    # 3. 语义加权 vs 降权
    if detected_topic != "General":
        # 💎 命中硬核板块：大幅加分
        base_score += 2000
        base_score *= 1.5
    else:
        # 📉 General 惩罚
        base_score *= 0.5 

    # 4. 政治排毒 (Nuclear Detox)
    has_noise = False
    for noise in NOISE_KEYWORDS:
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
            base_score *= 0.1 # 💣 无豁免的噪音
            detected_topic = "Politics" # 强制归类为(坏)政治
            
    # 5. VIP 加成
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
    
    # 4. 🛡️ 多样性熔断 (Diversity Breaker)
    final_list = []
    author_counts = {}
    
    for t in scored_tweets:
        if len(final_list) >= TARGET_TOTAL_QUOTA:
            break
            
        author = t['user_name']
        if author_counts.get(author, 0) >= 3:
            continue
            
        final_list.append(t)
        author_counts[author] = author_counts.get(author, 0) + 1
        
    # 5. 生成战报
    header = "| 信号 | 🏷️ 标签 | 热度 | 博主 | 摘要 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- | :--- |"
    rows = []
    
    for t in final_list:
        score_display = fmt_k(t['_score'])
        
        topic_raw = t['_topic']
        if topic_raw in ["General"]: 
            topic_str = topic_raw
        else: 
            topic_str = f"**{topic_raw}**"
        
        heat = f"❤️ {fmt_k(t.get('likes',0))}<br>🔁 {fmt_k(t.get('retweets',0))}" 
        
        user = t['user_name']
        text = t['full_text'].replace('\n', ' ')[:70] + "..."
        url = t['url']
        
        rows.append(f"| **{score_display}** | {topic_str} | {heat} | {user} | {text} | [🔗]({url}) |")

    return {"🏆 全域精选 (Top 30)": {"header": header, "rows": rows}}
