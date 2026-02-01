import json
import math
from datetime import datetime, timedelta

TABLE_NAME = "twitter_logs"

SECTORS = [
    "Crypto", "Tech", "Science", "Finance", 
    "Economy", "Geopolitics", "Politics"
]

QUOTA_HARDCORE = 20
QUOTA_WILDCARD = 5

# === 🛑 1. 政治噪音词 (触发降权) ===
# 只要出现这些词，系统首先会警觉：“这可能是垃圾口水仗”
POLITICAL_NOISE = [
    "woke", "maga", "democrat", "republican", "leftist", "right wing", "liberal", "conservative",
    "fascist", "communist", "socialist", "pronouns", "dei", "border crisis", "illegal",
    "trump", "biden", "harris", "vance", "pelosi", "schumer", "election", "ballot",
    "scandal", "epstein", "pedophile", "traitor", "shame", "disgrace", "culture war"
]

# === 🔰 2. 核心豁免词库 (免死金牌) ===
# 只要包含这些词，说明是在聊正事（宏观/立法/人事），立刻解除降权！
MACRO_IMMUNITY = [
    # --- 央行与宏观经济 ---
    "fed", "federal reserve", "powell", "fomc", "rate", "interest", "cut", "hike",
    "stimulus", "debt", "deficit", "budget", "tax", "treasury", "bond", "yield",
    
    # --- 贸易与地缘 ---
    "tariff", "trade war", "sanction", "export", "import", "duty",
    "china", "taiwan", "russia", "ukraine", "israel", "iran", "war", "military",
    
    # --- 核心资产监管 ---
    "bitcoin", "btc", "crypto", "ban", "regulation", "sec", "gensler", "etf",
    
    # --- 🔥 新增：核心政治动作 (Political Core) 🔥 ---
    "executive order", "veto", "sign", "bill", "act", "law", "legislation", # 立法/行政
    "nominate", "nominee", "appoint", "confirm", "sworn in", "cabinet", # 人事任免
    "resign", "step down", "impeach", "convict", "expel", # 重大变动
    "supreme court", "scotus", "ruling", "verdict", "unconstitutional", # 司法裁决
    "state of the union", "white house", "congress", "senate" # 核心机构
]

# === 🧠 3. 硬核关键词库 ===
KEYWORD_RULES = {
    "Crypto": [
        "bitcoin", "btc", "$btc", "eth", "ethereum", "solana", "$sol", 
        "defi", "nft", "stablecoin", "usdc", "usdt", "etf", "blackrock",
        "layer2", "zk-rollup", "airdrop", "staking", "restaking", "memecoin",
        "binance", "coinbase", "satoshi", "vitalik", "on-chain", "wallet"
    ],
    "Tech": [
        "ai", "genai", "llm", "transformer", "diffusion", "inference", "training",
        "gpt-5", "gpt-4", "claude", "gemini", "llama", "deepseek", "mistral",
        "nvidia", "$nvda", "h100", "blackwell", "cuda", "gpu", "tpu", "asic",
        "tsmc", "$tsm", "asml", "semiconductor", "chip", "wafer",
        "spacex", "starship", "falcon", "tesla", "$tsla", "fsd", "optimus",
        "python", "rust", "javascript", "github", "huggingface", "arxiv", "open source"
    ],
    "Science": [
        "nature journal", "science magazine", "arxiv", "paper", "peer review", "preprint",
        "nasa", "esa", "jwst", "supernova", "exoplanet", "quantum", "entanglement",
        "superconductor", "lk-99", "fusion", "iter", "plasma",
        "crispr", "mrna", "protein", "enzyme", "cancer", "alzheimer", "longevity", "aging"
    ],
    "Finance": [
        "sp500", "$spy", "nasdaq", "$qqq", "dow jones", "russell 2000",
        "10y yield", "treasury", "bond", "curve inversion",
        "gold", "xau", "silver", "crude oil", "brent", "natural gas",
        "earnings", "revenue", "guidance", "margin", "buyback", "dividend",
        "volatility", "vix", "liquidity", "repo"
    ],
    "Economy": [
        "fomc", "fed", "powell", "rate hike", "rate cut", "basis points", "bps",
        "cpi", "ppi", "pce", "inflation", "deflation", "stagflation",
        "gdp", "recession", "soft landing", "hard landing",
        "nfp", "non-farm", "unemployment", "jobless claims", "payroll",
        "debt ceiling", "deficit", "balance sheet", "qt", "qe"
    ],
    "Geopolitics": [
        "ukraine", "russia", "putin", "zelensky", "donbas", "kursk",
        "israel", "gaza", "hamas", "hezbollah", "iran", "tehran", "red sea", "houthi",
        "china", "xi jinping", "taiwan", "south china sea", "pla", "semiconductor sanction",
        "nato", "pentagon", "dod", "nuclear", "icbm", "drone warfare"
    ],
    "Politics": [
        "congress", "senate", "house", "bill", "legislation", "supreme court", "scotus",
        "executive order", "nominee"
    ]
}

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

# 🔥 核心：上帝权重算法 (逻辑更新) 🔥
def calculate_god_score(item, sector):
    metrics = item.get('raw_json', {}).get('metrics', {})
    text = (item.get('full_text') or "").lower()
    user = (item.get('user_name') or "")
    
    likes = metrics.get('likes', 0)
    retweets = metrics.get('retweets', 0)
    bookmarks = metrics.get('bookmarks', 0)
    base_score = (retweets * 5) + (bookmarks * 10) + likes
    
    # 1. 硬核加分
    if sector in ["Tech", "Science", "Crypto", "Finance", "Economy"]:
        base_score += 2000 
        base_score *= 1.5 
    
    # 2. 政治排毒机制 (Detox)
    has_noise = False
    for noise in POLITICAL_NOISE:
        if noise in text:
            has_noise = True
            break
            
    if has_noise:
        # 🛡️ 检查是否有“免死金牌”
        is_immune = False
        for safe_word in MACRO_IMMUNITY:
            if safe_word in text:
                is_immune = True
                break
        
        # 只有在【有噪音】且【无豁免权】时才降权
        if not is_immune:
            if sector == "Politics": base_score *= 0.5 
            else: base_score *= 0.1 
        # 如果有豁免权，不论有没有 Trump，都不降权，保留原分！

    # 3. 白名单
    for vip in VIP_AUTHORS:
        if vip.lower() in user.lower():
            base_score += 5000 
            break
            
    return base_score

def calculate_raw_heat(item):
    metrics = item.get('raw_json', {}).get('metrics', {})
    return (metrics.get('retweets', 0) * 2) + metrics.get('likes', 0)

def detect_sector(item):
    text = (item.get('full_text') or "").lower()
    user = (item.get('user_name') or "").lower()
    content_corpus = f"{text} {user}"
    for sector, keywords in KEYWORD_RULES.items():
        for k in keywords:
            if k in content_corpus: return sector
    for tag in item.get('tags', []):
        if tag in KEYWORD_RULES: return tag
    return None

def get_hot_items(supabase, table_name):
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_tweets = res.data if res.data else []
    except Exception as e: return {}

    if not all_tweets: return {}

    unique_map = {}
    for t in all_tweets:
        key = t.get('url') or (t.get('user_name'), t.get('full_text'))
        if key not in unique_map:
            unique_map[key] = t
    deduplicated = list(unique_map.values())

    # 双通道筛选
    pool_for_selection = []
    for t in deduplicated:
        target = detect_sector(t)
        if target:
            t['_god_score'] = calculate_god_score(t, target)
            t['_raw_heat'] = calculate_raw_heat(t)
            t['_sector'] = target
            pool_for_selection.append(t)
            
    # Top 20 精选
    pool_for_selection.sort(key=lambda x: x['_god_score'], reverse=True)
    top_hardcore = pool_for_selection[:QUOTA_HARDCORE]
    selected_urls = set([t['url'] for t in top_hardcore])
    
    # Top 5 兜底
    remaining_pool = [t for t in pool_for_selection if t['url'] not in selected_urls]
    remaining_pool.sort(key=lambda x: x['_raw_heat'], reverse=True)
    top_wildcard = remaining_pool[:QUOTA_WILDCARD]
    
    for t in top_wildcard:
        t['_is_wildcard'] = True
        
    final_roster = top_hardcore + top_wildcard
    
    sector_pools = {s: [] for s in SECTORS}
    for t in final_roster:
        if t['_sector'] in sector_pools:
            sector_pools[t['_sector']].append(t)
            
    intelligence_matrix = {}
    for sector, pool in sector_pools.items():
        if not pool: continue
        pool.sort(key=lambda x: x['_god_score'], reverse=True)
        
        header = "| 信号 | 热度指标 | 博主 | 摘要 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- |"
        rows = []
        for t in pool:
            score_display = fmt_k(t['_god_score'])
            if t.get('_is_wildcard'): score_display += " 🔥" 
            
            heat = f"❤️ {fmt_k(t.get('likes',0))}<br>🔁 {fmt_k(t.get('retweets',0))}" 
            user = t['user_name']
            text = t['full_text'].replace('\n', ' ')[:80] + "..."
            url = t['url']
            rows.append(f"| **{score_display}** | {heat} | {user} | {text} | [🔗]({url}) |")
        
        intelligence_matrix[sector] = {"header": header, "rows": rows}

    return intelligence_matrix
