import json
from datetime import datetime, timedelta

TABLE_NAME = "github_logs"

def fmt_k(num):
    if not num: return "-"
    try: n = float(num)
    except: return "-"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(int(n))

# ... (process 函数不变) ...
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
            "topics": i.get('tags', []),
            "raw_json": i 
        }
        refined_results.append(row)
    return refined_results

def get_hot_items(supabase, table_name):
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_repos = res.data if res.data else []
    except Exception as e: return {}
    if not all_repos: return {}

    unique_repos = {}
    for r in all_repos:
        name = r.get('repo_name')
        if not name: continue
        if name not in unique_repos or r['stars'] > unique_repos[name]['stars']:
            unique_repos[name] = r

    sector_pools = {}
    for repo in unique_repos.values():
        tags = repo.get('topics', [])
        if isinstance(tags, str):
            try: tags = json.loads(tags)
            except: tags = ["Uncategorized"]
        if not tags: tags = ["Uncategorized"]
        for tag in tags:
            if tag not in sector_pools: sector_pools[tag] = []
            sector_pools[tag].append(repo)

    matrix = {}
    for sector, pool in sector_pools.items():
        pool.sort(key=lambda x: x['stars'], reverse=True)
        
        # 🔥 GitHub 自定义表头
        header = "| Stars | 项目 | 标签 | 🔗 |\n| :--- | :--- | :--- | :--- |"
        rows = []
        for r in pool:
            stars = fmt_k(r['stars'])
            name = r['repo_name']
            
            raw_tags = r.get('topics', [])
            if isinstance(raw_tags, str):
                try: raw_tags = json.loads(raw_tags)
                except: raw_tags = []
            tags = ", ".join(raw_tags[:2])
            
            rows.append(f"| ⭐ {stars} | {name} | {tags} | [🔗]({r['url']}) |")
            
        matrix[sector] = {"header": header, "rows": rows}
        
    return matrix
