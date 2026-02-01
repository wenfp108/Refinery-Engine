import json
from datetime import datetime, timedelta

# === ⚙️ 1. 基础配置 ===
TABLE_NAME = "github_logs"

# === 🛠️ 2. 数据清洗 (入库) ===
def process(raw_data, path):
    if isinstance(raw_data, dict) and "items" in raw_data:
        items = raw_data["items"]
        meta_time = raw_data.get("meta", {}).get("scanned_at_bj")
    else:
        items = raw_data if isinstance(raw_data, list) else [raw_data]
        meta_time = None

    refined_results = []
    
    for i in items:
        bj_time = meta_time if meta_time else datetime.now().isoformat()
        
        row = {
            "bj_time": bj_time,
            "repo_name": i.get('name'),
            "url": i.get('url'),
            "stars": int(i.get('stars', 0)),
            "topics": i.get('tags', []),     # 入库时叫 topics
            "raw_json": i 
        }
        refined_results.append(row)
    return refined_results

# === 📤 3. 战报生成 (直通模式) ===
def get_hot_items(supabase, table_name):
    # 1. 拉取数据
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_repos = res.data if res.data else []
    except Exception as e:
        print(f"⚠️ GitHub 数据拉取失败: {e}")
        return {}

    if not all_repos: return {}

    # 2. 简单去重
    unique_repos = {}
    for r in all_repos:
        name = r.get('repo_name')
        if not name: continue
        if name not in unique_repos or r['stars'] > unique_repos[name]['stars']:
            unique_repos[name] = r

    # 3. 分组
    sector_pools = {}
    for repo in unique_repos.values():
        tags = repo.get('topics', [])
        if isinstance(tags, str):
            try: tags = json.loads(tags)
            except: tags = ["Uncategorized"]
        if not tags: tags = ["Uncategorized"]

        for tag in tags:
            if tag not in sector_pools:
                sector_pools[tag] = []
            sector_pools[tag].append(repo)

    # 4. 生成输出
    intelligence_matrix = {}
    
    for sector, pool in sector_pools.items():
        pool.sort(key=lambda x: x['stars'], reverse=True)
        
        display_items = []
        for r in pool:
            # 获取标签用于显示
            raw_tags = r.get('topics', [])
            if isinstance(raw_tags, str):
                try: raw_tags = json.loads(raw_tags)
                except: raw_tags = []

            display_items.append({
                "score": r['stars'],
                "user_name": "GitHub",
                "full_text": r['repo_name'], 
                "url": r['url'],
                "tags": raw_tags  # 🔥 关键：把标签传给 Refinery
            })
            
        intelligence_matrix[sector] = display_items

    return intelligence_matrix
