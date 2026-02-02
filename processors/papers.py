import json
from datetime import datetime, timedelta

# 对应 Supabase 里的表名
TABLE_NAME = "papers_logs"

def fmt_k(num):
    if not num: return "0"
    try: n = float(num)
    except: return "0"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(int(n))

# === 1. 数据清洗逻辑 (保持不变) ===
def process(raw_data, path):
    data = raw_data if isinstance(raw_data, dict) else {}
    items = data.get("items", [])
    meta = data.get("meta", {})
    
    scanned_at = meta.get("scanned_at_bj")
    if not scanned_at:
        scanned_at = datetime.now().isoformat()
        
    refined_results = []
    for i in items:
        metrics = i.get("metrics", {})
        row = {
            "bj_time": scanned_at,
            "title": i.get("title"),
            "journal": i.get("journal"),
            # 区分 NUCLEAR (核爆) 和 EARLY (前沿)
            "signal_type": i.get("type", "General"), 
            "citations": int(metrics.get("citations", 0)),
            "impact_factor": float(metrics.get("impact_factor", 0.0)),
            "strategies": i.get("strategies", []), 
            "url": i.get("url"),
            "reason": i.get("reason"),
            "raw_json": i
        }
        refined_results.append(row)
    return refined_results

# === 2. 战报生成逻辑 (🔥 修改：3核爆 + 7前沿) ===
def get_hot_items(supabase, table_name):
    # 获取最近 24 小时数据
    yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
    try:
        res = supabase.table(table_name).select("*").gt("bj_time", yesterday).execute()
        all_papers = res.data if res.data else []
    except Exception as e:
        print(f"Papers DB Error: {e}")
        return {}
    
    if not all_papers: return {}

    # A. 去重 (保留引用更高的版本)
    unique_map = {}
    for p in all_papers:
        title = p.get("title")
        if not title: continue
        if title not in unique_map or p.get("citations", 0) > unique_map[title].get("citations", 0):
            unique_map[title] = p
    papers = list(unique_map.values())
    
    # 🔥 B. 咨询顾问筛选法 (Consultant's Filter)
    
    # 1. ☢️ Nuclear Pool (核爆池) - 只要前 3 个
    # 这些是必须知道的大事件
    nuclear_pool = [p for p in papers if "NUCLEAR" in p.get("signal_type", "")]
    nuclear_pool.sort(key=lambda x: x.get("citations", 0), reverse=True)
    final_nuclear = nuclear_pool[:3]

    # 2. ⚡ Frontier Pool (前沿池) - 只要前 7 个
    # 核心逻辑：找 "EARLY" 信号，或者是带有特定策略标签的论文
    # 如果引用数不高但被标记为 EARLY，说明它是潜力股
    def frontier_score(p):
        score = 0
        # 只要是 Early 就给高分，压倒普通的高引用论文
        if "EARLY" in p.get("signal_type", ""): score += 10000
        # 有策略标签（如 BIO_REVOLUTION）加分
        if p.get("strategies"): score += 5000
        # 最后才看引用数，作为辅助
        score += p.get("citations", 0)
        return score

    # 排除掉已经选入 Nuclear 的
    nuclear_ids = {p['title'] for p in final_nuclear}
    remaining_papers = [p for p in papers if p['title'] not in nuclear_ids]
    
    remaining_papers.sort(key=frontier_score, reverse=True)
    final_frontier = remaining_papers[:7]

    # 合并列表
    final_display_list = final_nuclear + final_frontier

    if not final_display_list: return {}

    # C. 构建 Markdown (增加 "潜力" 视觉提示)
    header = "| 信号 | 标题 | 引用 | 领域 (潜力方向) | 🔗 |\n| :--- | :--- | :--- | :--- | :--- |"
    rows = []
    
    for p in final_display_list:
        s_type = p.get("signal_type","")
        
        # 图标逻辑
        if "NUCLEAR" in s_type:
            icon = "☢️ **NUCLEAR**"
        elif "EARLY" in s_type:
            icon = "⚡ **Early**"  # 强调 Early
        else:
            icon = "📄 Paper"
            
        title = p.get("title", "")
        # 稍微缩短标题，让表格更整洁
        if len(title) > 60: title = title[:60] + "..."
            
        cite = fmt_k(p.get("citations", 0))
        
        # 标签处理：只显示最有价值的 1-2 个标签
        tags = p.get("strategies", [])
        if isinstance(tags, str):
            try: tags = json.loads(tags)
            except: tags = []
        
        # 视觉优化：用代码块高亮标签，一眼看到 "BIO", "AI" 等关键词
        if tags:
            tag_str = " ".join([f"`{t}`" for t in tags[:2]])
        else:
            tag_str = "-"
        
        url = p.get("url", "#")
        
        rows.append(f"| {icon} | {title} | {cite} | {tag_str} | [🔗]({url}) |")
        
    return {"🔬 Science Radar (科研前哨)": {"header": header, "rows": rows}}
