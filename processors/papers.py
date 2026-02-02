# === 修改 processors/papers.py 的 get_hot_items 函数 ===

def get_hot_items(supabase, table_name):
    # ... (前面的查询逻辑保持不变) ...
    
    # 假设 all_papers 已经获取到了
    if not all_papers: return {}

    # 1. 去重逻辑 (保持不变)
    unique_map = {}
    for p in all_papers:
        title = p.get("title")
        if not title: continue
        unique_map[title] = p
    papers = list(unique_map.values())
    
    # 🔥 2. 分组过滤策略 (核心修改)
    # 目标：3个 Nuclear (核爆) + 10个 Top Citations (高引/早期)
    
    # A. 挑出 NUCLEAR (核爆级)
    nuclear_list = [p for p in papers if "NUCLEAR" in p.get("signal_type", "")]
    # 按引用数降序，防止核爆太多（虽然很少见）
    nuclear_list.sort(key=lambda x: x.get("citations", 0), reverse=True)
    # 建议：如果真有核爆，有多少显示多少，或者限制前 3-5 个
    final_nuclear = nuclear_list[:3] 

    # B. 挑出其余的 (去除已选的 Nuclear)
    other_list = [p for p in papers if "NUCLEAR" not in p.get("signal_type", "")]
    # 按引用数降序 (或者你可以结合 impact_factor)
    other_list.sort(key=lambda x: x.get("citations", 0), reverse=True)
    # 截取前 10 个
    final_others = other_list[:10]

    # C. 合并列表 (Nuclear 永远在最前)
    final_display_list = final_nuclear + final_others

    # 3. 构建 Markdown 表格 (遍历 final_display_list)
    header = "| 信号 | 标题 | 引用 | 标签 (关键词) | 🔗 |\n| :--- | :--- | :--- | :--- | :--- |"
    rows = []
    
    for p in final_display_list:
        # ... (内部渲染逻辑保持不变，参考原文件 80-105 行) ...
        # 注意：这里只展示 final_display_list 里的内容
        s_type = p.get("signal_type","")
        if "NUCLEAR" in s_type:
            icon = "☢️ **NUCLEAR**"
        elif "EARLY" in s_type:
            icon = "⚡ Early"
        else:
            icon = "📄 Paper"
            
        title = p.get("title", "")
        if len(title) > 65: title = title[:65] + "..."
            
        cite = fmt_k(p.get("citations", 0))
        
        tags = p.get("strategies", [])
        if isinstance(tags, str):
            try: tags = json.loads(tags)
            except: tags = []
        tag_str = ", ".join([f"**{t}**" for t in tags[:2]])
        
        url = p.get("url", "#")
        rows.append(f"| {icon} | {title} | {cite} | {tag_str} | [🔗]({url}) |")
        
    return {"🔬 Science Radar (科研前哨)": {"header": header, "rows": rows}}
