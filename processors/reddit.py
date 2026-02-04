import json
from datetime import datetime, timedelta

# === 配置区 ===
# 对应 Supabase 里的表名 (记得去 Supabase SQL Editor 执行建表语句)
TABLE_NAME = "reddit_logs"

# 目标金融/科技板块 (用于"市场风向"策略筛选)
TARGET_MARKET_SUBS = [
    'wallstreetbets', 'stocks', 'economy', 'options', 'bitcoin', 
    'technology', 'hardware', 'semiconductors', 'futurology', 'investing'
]

# === 0. 辅助工具 ===
def fmt_k(num):
    """ 将数字格式化为 K/M (e.g. 1.2K, 15M) """
    if not num: return "0"
    try: n = float(num)
    except: return "0"
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(int(n))

# === 1. 数据清洗逻辑 (ETL) ===
# 负责解析 sentiment/ 目录下那种嵌套的 JSON 结构，并展平为数据库行
def process(raw_data, path):
    # 兼容处理：如果外层是列表（标准结构），取列表；如果是字典，包一层
    items = raw_data if isinstance(raw_data, list) else [raw_data]
    
    refined_results = []
    
    for batch in items:
        # 1. 提取批次时间 (JSON 里的 timestamp)
        # 格式示例: "2026-02-05T01:23:38.695680+08:00"
        ts = batch.get('timestamp')
        if not ts: ts = datetime.now().isoformat()
        
        # 2. 遍历板块 (data 列表)
        for sub_data in batch.get('data', []):
            subreddit = sub_data.get('subreddit')
            
            # 3. 遍历冠军帖子 (champions 列表)
            for post in sub_data.get('champions', []):
                # 构造数据库行结构
                row = {
                    "bj_time": ts,
                    "subreddit": subreddit,
                    "title": post.get('title'),
                    "url": post.get('url'),
                    "summary": post.get('summary'),
                    "score": int(post.get('score', 0)),
                    "vibe": float(post.get('vibe', 0.0)),
                    "raw_json": post  # 备份原始数据以备后用
                }
                refined_results.append(row)
                
    return refined_results

# === 2. 战报生成逻辑 (分类独立版) ===
def get_hot_items(supabase, table_name):
    # A. 获取最近 24 小时的数据
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        # 查库：按时间倒序
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_posts = res.data if res.data else []
    except Exception as e:
        print(f"Reddit DB Error: {e}")
        return {}
    
    if not all_posts: return {}

    # B. 去重逻辑 (Deduplication)
    # 同一个 URL 可能在不同时间点被抓取多次，我们只保留时间戳最新的那个
    unique_map = {}
    for p in all_posts:
        url = p.get('url')
        if not url: continue  # 丢弃没有 URL 的脏数据
        
        # 如果 URL 已存在，且当前这条的时间更新，则覆盖
        if url not in unique_map or p['bj_time'] > unique_map[url]['bj_time']:
            unique_map[url] = p
            
    posts = list(unique_map.values())

    # C. 分类筛选器 (The Filter Pipeline)
    
    # --- 策略 1: 🚨 全球绝对热点 (Viral Hits) ---
    # 逻辑：不分板块，全网 Score 最高的前 5 名
    viral_pool = sorted(posts, key=lambda x: x.get('score', 0), reverse=True)[:5]
    viral_ids = {p['url'] for p in viral_pool}  # 记录 ID 以免后续重复选入

    # --- 策略 2: 📉 市场与科技信号 (Market Movers) ---
    # 逻辑：只看特定金融/科技板块，排除已入选 Viral 的
    market_pool = [
        p for p in posts 
        if p.get('subreddit') in TARGET_MARKET_SUBS 
        and p['url'] not in viral_ids
    ]
    market_pool.sort(key=lambda x: x.get('score', 0), reverse=True)
    market_top = market_pool[:5]  # 取前 5

    # D. 构建 Markdown 表格 (分开展示)
    report_sections = {}

    def build_table(items, show_summary=False):
        if show_summary:
            # 市场风向：带摘要，方便看逻辑
            header = "| 热度 | r/板块 | 标题 & 摘要 | 情绪 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- |"
        else:
            # 全球热搜：只看标题，追求简洁
            header = "| 热度 | r/板块 | 标题 | 情绪 | 🔗 |\n| :--- | :--- | :--- | :--- | :--- |"
            
        rows = []
        for p in items:
            score = fmt_k(p.get('score', 0))
            sub = p.get('subreddit', 'unknown')
            title = p.get('title', '-').replace('|', '')[:50] + "..."
            
            # 情绪 Emoji
            vibe_val = p.get('vibe', 0)
            if vibe_val > 0.2: vibe_icon = "😍"
            elif vibe_val < -0.2: vibe_icon = "😡"
            else: vibe_icon = "😐"
            
            url = p.get('url', '#')
            
            if show_summary:
                # 摘要处理：让表格内容更丰富
                summary = p.get('summary', '').replace('\n', ' ')[:80] + "..."
                content_col = f"**{title}**<br>_{summary}_"
            else:
                content_col = f"**{title}**"

            rows.append(f"| {score} | `{sub}` | {content_col} | {vibe_icon} {vibe_val:.2f} | [🔗]({url}) |")
            
        return {"header": header, "rows": rows}

    # 组装战报：分类独立展示
    if viral_pool:
        report_sections["🚨 Reddit Viral (全球热搜)"] = build_table(viral_pool, show_summary=False)
    
    if market_top:
        report_sections["📉 Market & Tech (市场风向)"] = build_table(market_top, show_summary=True)

    return report_sections
