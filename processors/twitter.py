import json
import math
from datetime import datetime, timedelta

TABLE_NAME = "twitter_logs"
TARGET_TOTAL_QUOTA = 30  # 🌟 最终只选出全网最好的 30 条

# === 🛑 1. 政治/垃圾噪音词 (核打击) ===
# 只要出现这些词，分数直接打 1 折（除非有豁免权）
NOISE_KEYWORDS = [
    "woke", "maga", "democrat", "republican", "leftist", "right wing", "liberal", "conservative",
    "fascist", "communist", "socialist", "pronouns", "dei", "border crisis", "illegal",
    "trump", "biden", "harris", "vance", "pelosi", "schumer", "election", "ballot",
    "scandal", "epstein", "pedophile", "traitor", "shame", "disgrace", "culture war",
    "nazi", "hitler", "antisemitism", "zionist", "genocide"
]

# === 🔰 2. 宏观豁免词 (免死金牌) ===
# 政治贴里如果有这些词，说明在聊正事（立法/宏观/监管），不降权
MACRO_IMMUNITY = [
    "fed", "federal reserve", "powell", "fomc", "rate", "interest", "cut", "hike",
    "tariff", "trade war", "sanction", "export", "import", "duty",
    "china", "taiwan", "russia", "ukraine", "israel", "iran", "war", "military",
    "stimulus", "debt", "deficit", "budget", "tax", "treasury", "bond", "yield",
    "bitcoin", "btc", "crypto", "ban", "regulation", "sec", "gensler", "etf",
    "executive order", "veto", "sign", "bill", "act", "law", "legislation",
    "nominate", "nominee", "appoint", "confirm", "supreme court", "ruling"
]

# === 🧠 3. 精准话题词库 (权重竞价模式) ===
# 词越长、越专业，权重越高，防止误判
TOPIC_RULES = {
    "Crypto": [
        "bitcoin", "btc", "ethereum", "eth", "solana", "defi", "nft", "stablecoin", "usdc", "usdt",
        "etf flow", "blackrock", "layer2", "zk-rollup", "airdrop", "staking", "restaking", "memecoin",
        "binance", "coinbase", "satoshi", "vitalik", "on-chain analysis", "wallet", "altcoin"
    ],
    "AI/Tech": [
        "llm", "transformer", "genai", "generative ai", "inference", "training run", "pre-training",
        "gpt-5", "gpt-4", "claude", "gemini", "llama", "deepseek", "mistral", "anthropic", "openai",
        "nvidia", "nvda", "h100", "blackwell", "cuda", "gpu", "tpu", "asic", "compute",
        "tsmc", "asml", "semiconductor", "chip", "wafer", "moore's law",
        "spacex", "starship", "falcon", "tesla", "tsla", "fsd", "optimus", "robot",
        "python", "rust", "github", "huggingface", "arxiv", "open source"
    ],
    "Science": [
        "nature journal", "science magazine", "arxiv", "peer review", "preprint",
        "nasa", "esa", "jwst", "supernova", "exoplanet", "quantum", "entanglement",
        "superconductor", "lk-99", "fusion energy", "iter", "plasma",
        "crispr", "mrna", "protein", "enzyme", "cancer research", "alzheimer", "longevity"
    ],
    "Macro": [
        "sp500", "nasdaq", "bond yield", "treasury", "curve inversion",
        "gold", "xau", "silver", "crude oil", "brent", "natural gas",
        "earnings call", "revenue", "guidance", "profit margin", "buyback", "dividend",
        "fomc", "fed funds", "powell", "cpi", "ppi", "pce", "inflation", "deflation", "stagflation",
        "gdp", "recession", "soft landing", "non-farm", "unemployment", "jobless", "payroll",
        "balance sheet", "quantitative tightening", "liquidity injection"
    ],
    "Geo": [
        "ukraine", "russia", "putin", "zelensky", "donbas", "kursk",
        "israel", "gaza", "hamas", "hezbollah", "iran", "tehran", "red sea", "houthi",
        "china", "xi jinping", "taiwan", "south china sea", "pla", "semiconductor sanction",
        "nato", "pentagon", "dod", "nuclear", "icbm", "drone warfare"
    ]
}

# === 🛡️ 4. VIP 白名单 (基础分加成) ===
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

# 🔥 核心：上帝权重算法 3.0 🔥
def calculate_score_and_tag(item):
    text = (item.get('full_text') or "").lower()
    user = (item.get('user_name') or "")
    
    # 1. 基础热度 (书签 x10, 转推 x5, 点赞 x1)
    # 书签权重最高，因为它代表深度阅读和收藏价值
    metrics = item.get('raw_json', {}).get('metrics', {})
    base_score = (metrics.get('retweets', 0) * 5) + \
                 (metrics.get('bookmarks', 0) * 10) + \
                 metrics.get('likes', 0)
    
    # 2. 话题竞价 (解决分类幻觉)
    detected_topic = "General"
    max_keyword_len = 0 # 匹配到的关键词越长，置信度越高
    
    for topic, keywords in TOPIC_RULES.items():
        for k in keywords:
            # 必须匹配到关键词才算
            if k in text:
                # 优先级逻辑：保留匹配到的最长/最具体的关键词所属的话题
                if len(k) > max_keyword_len:
                    detected_topic = topic
                    max_keyword_len = len(k)
    
    # 3. 语义加权 vs 降权
    if detected_topic != "General":
        # 💎 命中硬核板块：大幅加分
        base_score += 2000
        base_score *= 1.5
    else:
        # 📉 General 惩罚：没营养的水贴，分数打对折
        # 防止马斯克的普通推文刷屏
        base_score *= 0.5 

    # 4. 政治排毒 (Nuclear Detox)
    has_noise = False
    for noise in NOISE_KEYWORDS:
        if noise in text:
            has_noise = True
            break
            
    if has_noise:
        # 检查是否有免死金牌 (宏观豁免)
        is_immune = False
        for safe in MACRO_IMMUNITY:
            if safe in text:
                is_immune = True
                break
        
        if not is_immune:
            base_score *= 0.1 # 💣 无豁免的政治噪音，直接打1折
            detected_topic = "Politics" # 强制标记
            
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
    # 限制单人霸榜，每人最多保留前 3 条
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
        
    # 5. 生成战报 (单张大表)
    header = "| 信号 | 🏷️ 标签 | 热度 | 博主 | 摘要 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- | :--- |"
    rows = []
    
    for t in final_list:
        score_display = fmt_k(t['_score'])
        
        # 标签美化
        topic_raw = t['_topic']
        if topic_raw == "General": 
            topic_str = "General" 
        elif topic_raw == "Politics":
            topic_str = "Politics"
        else: 
            topic_str = f"**{topic_raw}**" # 硬核标签加粗显示
        
        # 热度垂直排版
        heat = f"❤️ {fmt_k(t.get('likes',0))}<br>🔁 {fmt_k(t.get('retweets',0))}" 
        
        user = t['user_name']
        # 智能摘要：截取前70字符，去除换行
        text = t['full_text'].replace('\n', ' ')[:70] + "..."
        url = t['url']
        
        rows.append(f"| **{score_display}** | {topic_str} | {heat} | {user} | {text} | [🔗]({url}) |")

    # 返回给 Refinery 的统一格式
    return {"🏆 全域精选 (Top 30)": {"header": header, "rows": rows}}
