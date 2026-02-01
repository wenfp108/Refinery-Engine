import json
import math
from datetime import datetime, timedelta

# ==========================================
# ⚙️ 配置区 (V4.3 - Fix URL & Enhance Macro)
# ==========================================

TABLE_NAME = "twitter_logs"
TARGET_TOTAL_QUOTA = 30 

# === 🛑 1. 政治/垃圾噪音词 ===
NOISE_KEYWORDS = [
    "woke", "libtard", "magatard", "shame", "disgrace", "traitor", 
    "pedophile", "epstein", "pronouns", "culture war", "scandal",
    "destroy", "lies", "liar", "clown", "hypocrite", "idiot", "scam"
]

# === 🔰 2. 宏观豁免词 (保护 Dalio 这种长文博主) ===
MACRO_IMMUNITY = [
    "fed", "federal reserve", "powell", "fomc", "rate", "interest", "cut", "hike",
    "tariff", "trade war", "sanction", "export", "import", "duty",
    "china", "taiwan", "russia", "ukraine", "israel", "iran", "war", "military",
    "stimulus", "debt", "deficit", "budget", "tax", "treasury", "bond", "yield",
    "bitcoin", "btc", "crypto", "ban", "regulation", "sec", "etf",
    "executive order", "veto", "sign", "bill", "act", "law", "legislation",
    "nominate", "nominee", "appoint", "confirm", "supreme court", "ruling",
    "productivity", "cycle", "credit", "bubble", "deleveraging" # 🌟 Dalio 专用词
]

# === 🧠 3. 精准话题词库 ===
TOPIC_RULES = {
    "Tech": [ 
        "llm", "genai", "gpt-5", "gpt-4", "claude", "gemini", "llama", "deepseek", "anthropic", "openai",
        "nvidia", "nvda", "h100", "blackwell", "cuda", "gpu", "semiconductor", "tsmc", "asml", "wafer",
        "spacex", "starship", "falcon", "tesla", "tsla", "fsd", "robot", "optimus", "figure ai",
        "python", "rust", "github", "huggingface", "open source", "coding", "developer"
    ],
    "Politics": [ 
        "white house", "biden", "trump", "harris", "vance", "congress", "senate", "house of rep",
        "supreme court", "scotus", "legislation", "bill", "veto", "executive order", "amendment",
        "election", "poll", "voter", "ballot", "campaign", "republican", "democrat", "gop", "dnc",
        "regulator", "antitrust", "doj", "ftc", "sec chairman"
    ],
    "Finance": [ 
        "sp500", "nasdaq", "spx", "ndx", "dow jones", "russell 2000", "vix", "volatility",
        "stock", "equity", "earnings", "revenue", "margin", "guidance", "buyback", "dividend",
        "goldman", "jpmorgan", "morgan stanley", "bloomberg", "blackrock", "citadel", "bridgewater",
        "ipo", "merger", "acquisition", "short seller", "long position", "call option", "put option",
        "liquidity", "market maker", "hedge fund", "pension fund"
    ],
    "Economy": [ # 🌟 加强了 Kevin Warsh 和 Dalio 常用词
        "fomc", "federal reserve", "jerome powell", "fed funds", "interest rate", "hike", "cut",
        "cpi", "ppi", "pce", "inflation", "deflation", "stagflation", "recession", "soft landing",
        "gdp", "unemployment", "jobless", "jolts", "non-farm", "payroll", "labor market",
        "treasury", "bond", "yield", "10y", "2y", "curve inversion", "debt ceiling", "deficit",
        "ism", "pmi", "retail sales", "housing start",
        "kevin warsh", "warsh", "productivity", "long term debt", "monetary policy"
    ],
    "Geo": [ 
        "ukraine", "russia", "putin", "zelensky", "kursk", "kyiv",
        "israel", "gaza", "hamas", "iran", "tehran", "red sea", "houthi", "hezbollah",
        "china", "xi jinping", "taiwan", "south china sea", "pla", "ccp",
        "nato", "pentagon", "nuclear", "weapon", "sanction", "trade war", "tariff"
    ],
    "Science": [ 
        "nature journal", "science magazine", "arxiv", "peer review", "preprint",
        "nasa", "esa", "jwst", "supernova", "exoplanet", "quantum", "fusion energy", "lk-99",
        "crispr", "mrna", "cancer", "alzheimer", "longevity", "biology", "physics", "chemistry"
    ],
    "Crypto": [ 
        "bitcoin", "btc", "ethereum", "eth", "solana", "defi", "stablecoin", "usdc", "usdt",
        "etf flow", "blackrock", "coinbase", "binance", "satoshi", "vitalik", "memecoin", "doge",
        "wallet", "private key", "smart contract", "layer2", "zk-rollup", "airdrop",
        "mstr", "microstrategy", "michael saylor"
    ]
}

# === 🛡️ 4. VIP 白名单 ===
VIP_AUTHORS = [
    "Karpathy", "Yann LeCun", "Paul Graham", "Sam Altman", "François Chollet", 
    "Rowan Cheung", "Naval", "Palmer Luckey", "Anduril", "Elon Musk",
    "Nick Timiraos", "Ray Dalio", "Mohamed A. El-Erian", "Kobeissi Letter", 
    "Walter Bloomberg", "Zerohedge", "Lyn Alden", "MacroAlf", "Goldman Sachs",
    "Peter Schiff", "Michael Saylor", "Nassim Nicholas Taleb", "CME Group",
    "Fitch Ratings", "IMF", "Unusual Whales", "The Economist", "WSJ Central Banks",
    "Ian Bremmer", "Eric Topol", "Vitalik", "SentDefender", "Visegrád 24",
    "Spectator Index", "Disclose.tv", "Defense News", "Council on Foreign Relations"
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

# ✅ Process: 严格映射到 SQL 的 'url' 列
def process(raw_data, path):
    items = raw_data if isinstance(raw_data, list) else [raw_data]
    refined_results = []
    
    for i in items:
        text = i.get('fullText', '')
        if len(text) < 10 and 'http' not in text:
            continue

        user = i.get('user', {})
        metrics = i.get('metrics', {})
        
        # 提取增长数据
        growth_views = i.get('growth_views', 0)
        growth_likes = i.get('growth_likes', 0)
        growth_retweets = i.get('growth_retweets', 0)
        growth_replies = i.get('growth_replies', 0)

        row = {
            "bj_time": to_iso_bj(i.get('createdAt')),
            "user_name": user.get('name'),
            "screen_name": user.get('screenName'),
            "followers_count": user.get('followersCount'),
            "full_text": text,
            
            # 🔥 关键修正：确保写入 'url' 列
            "url": i.get('tweetUrl'), 
            
            "tags": json.dumps(i.get('tags', [])),
            
            "likes": metrics.get('likes', 0),
            "retweets": metrics.get('retweets', 0),
            "replies": metrics.get('replies', 0),
            "quotes": metrics.get('quotes', 0),
            "bookmarks": metrics.get('bookmarks', 0),
            "views": i.get('views', metrics.get('viewCount', 0)),
            
            "growth_views": growth_views,
            "growth_likes": growth_likes,
            "growth_retweets": growth_retweets,
            "growth_replies": growth_replies,
            
            "raw_json": json.dumps(i) if isinstance(i, dict) else i
        }
        refined_results.append(row)
        
    return refined_results

def calculate_score_and_tag(item):
    text = (item.get('full_text') or "").lower()
    user = (item.get('user_name') or "")
    
    base_score = (item.get('retweets', 0) * 5) + \
                 (item.get('bookmarks', 0) * 10) + \
                 item.get('likes', 0)
    
    detected_topic = "General"
    max_keyword_len = 0
    
    for topic, keywords in TOPIC_RULES.items():
        for k in keywords:
            if k in text:
                if len(k) > max_keyword_len:
                    detected_topic = topic
                    max_keyword_len = len(k)
    
    if detected_topic != "General":
        base_score += 2000
        base_score *= 1.5
    else:
        base_score *= 0.5 

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
            base_score *= 0.1 
            detected_topic = "Politics" 
            
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
    except Exception as e:
        print(f"Database error: {e}")
        return {}

    if not all_tweets: return {}

    unique_map = {}
    for t in all_tweets:
        # 🔥 读取修正：从 'url' 读取
        key = t.get('url') or (t.get('user_name'), t.get('full_text'))
        if key not in unique_map:
            unique_map[key] = t
    tweets = list(unique_map.values())

    scored_tweets = []
    for t in tweets:
        score, topic = calculate_score_and_tag(t)
        t['_score'] = score
        t['_topic'] = topic
        scored_tweets.append(t)
        
    scored_tweets.sort(key=lambda x: x['_score'], reverse=True)
    
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
        
    header = "| 信号 | 🏷️ 标签 | 热度 | 博主 | 摘要 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- | :--- |"
    rows = []
    
    for t in final_list:
        score_display = fmt_k(t['_score'])
        topic_raw = t['_topic']
        
        if topic_raw in ["General", "Politics"]: 
            topic_str = topic_raw 
        else: 
            topic_str = f"**{topic_raw}**"
        
        heat = f"❤️ {fmt_k(t.get('likes',0))}<br>🔁 {fmt_k(t.get('retweets',0))}" 
        user = t['user_name']
        text = t['full_text'].replace('\n', ' ')[:70] + "..."
        url = t.get('url', '#')
        
        rows.append(f"| **{score_display}** | {topic_str} | {heat} | {user} | {text} | [🔗]({url}) |")

    return {"🏆 全域精选 (Top 30)": {"header": header, "rows": rows}}
