import os, json, base64, requests, importlib.util, sys
from datetime import datetime, timedelta, timezone
import pandas as pd
from supabase import create_client
from github import Github, Auth

# === 🛡️ 1. 核心配置 ===
PRIVATE_BANK_ID = "wenfp108/Central-Bank" 
GITHUB_TOKEN = os.environ.get("GH_PAT") 
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([GITHUB_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    sys.exit("❌ [审计异常] 环境变量缺失。")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
auth = Auth.Token(GITHUB_TOKEN)
gh_client = Github(auth=auth)
private_repo = gh_client.get_repo(PRIVATE_BANK_ID)

# === 🧩 2. 插件发现系统 ===
def get_all_processors():
    procs = {}
    proc_dir = "./processors"
    if not os.path.exists(proc_dir): return procs
    for filename in os.listdir(proc_dir):
        if filename.endswith(".py") and not filename.startswith("__"):
            name = filename[:-3]
            try:
                spec = importlib.util.spec_from_file_location(f"mod_{name}", os.path.join(proc_dir, filename))
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                procs[name] = {
                    "module": mod,
                    "table_name": getattr(mod, "TABLE_NAME", f"{name}_logs"),
                }
            except Exception as e: print(f"⚠️ 插件 {name} 加载失败: {e}")
    return procs

# === ⏱️ 辅助：检查数据新鲜度 ===
def get_data_freshness(table_name):
    try:
        res = supabase.table(table_name).select("bj_time").order("bj_time", desc=True).limit(1).execute()
        if not res.data: return (False, 9999, "无数据")
        
        last_time_str = res.data[0]['bj_time']
        if not last_time_str: return (False, 9999, "无时间戳")

        try:
            last_time_str = last_time_str.replace('Z', '+00:00')
            last_time = datetime.fromisoformat(last_time_str)
        except:
            return (False, 9999, last_time_str)
        
        now = datetime.now(timezone(timedelta(hours=8)))
        if last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=timezone(timedelta(hours=8)))
        
        diff = now - last_time
        minutes_ago = int(diff.total_seconds() / 60)
        
        return (minutes_ago <= 65, minutes_ago, last_time.strftime('%H:%M'))
    except Exception as e:
        print(f"⚠️ 新鲜度检查失败 ({table_name}): {e}")
        return (True, 0, "CheckError")

# === 🔥 3. 战报工厂：Markdown 垂直堆叠引擎 ===

def generate_hot_reports(processors_config):
    print("\n🔥 [情报对冲] 正在生成 Markdown 时报...")
    bj_now = datetime.now(timezone(timedelta(hours=8)))
    
    # 🔥 [修改点 1] 自定义文件名格式: 2026-02-01-14.md
    # 使用中划线分隔，方便阅读
    file_name = bj_now.strftime('%Y-%m-%d-%H') + ".md"
    report_path = f"reports/{file_name}"
    
    date_display = bj_now.strftime('%Y-%m-%d %H:%M')
    
    md_report = f"# 🚀 Architect's Alpha 情报审计 ({date_display})\n\n"
    md_report += "> **机制说明**：全源智能去重 | 无更新源自动折叠\n\n"

    has_content = False

    for source_name, config in processors_config.items():
        if hasattr(config["module"], "get_hot_items"):
            try:
                table = config["table_name"]
                is_fresh, mins_ago, last_update_time = get_data_freshness(table)
                
                # 睡眠模式
                if not is_fresh:
                    md_report += f"## 💤 来源：{source_name.upper()} (上次更新: {last_update_time})\n"
                    md_report += f"> *距上次更新已过 {int(mins_ago/60)} 小时，暂无新数据。*\n\n"
                    continue 

                # 唤醒模式
                sector_matrix = config["module"].get_hot_items(supabase, table)
                if not sector_matrix: continue

                has_content = True
                md_report += f"## 📡 来源：{source_name.upper()}\n"
                
                for sector, items in sector_matrix.items():
                    md_report += f"### 🏷️ 板块：{sector}\n"
                    md_report += "| 信号强度 | 源头 | 关键情报摘要 | 链接 |\n| :--- | :--- | :--- | :--- |\n"
                    
                    for item in items:
                        score = int(item.get('score', 0))
                        source = item.get('user_name', 'Unknown')
                        text = item.get('full_text', '').replace('\n', ' ')[:85] + "..."
                        url = item.get('url') or item.get('tweet_url') or '#'
                        
                        md_report += f"| **{score:,}** | {source} | {text} | [查看]({url}) |\n"
                    md_report += "\n"
            except Exception as e:
                print(f"⚠️ {source_name} 渲染异常: {e}")

    if not has_content:
        md_report += "\n\n**🛑 本轮扫描全域静默，请查阅历史归档。**"

    # 🔥 [修改点 2] 只写入您指定的这一个文件，不再写 latest_brief.md
    try:
        try:
            old = private_repo.get_contents(report_path)
            private_repo.update_file(old.path, f"📊 Update: {file_name}", md_report, old.sha)
            print(f"✅ 更新战报: {report_path}")
        except:
            private_repo.create_file(report_path, f"🚀 New: {file_name}", md_report)
            print(f"✅ 创建战报: {report_path}")
    except Exception as e: print(f"❌ 写入 {report_path} 失败: {e}")

# === 🏦 5. 搬运逻辑 (支持全量补录) ===

def process_and_upload(path, sha, config):
    # 查重：如果文件已处理，秒退
    check = supabase.table("processed_files").select("file_sha").eq("file_sha", sha).execute()
    if check.data: return False 
    
    print(f"📥 正在处理: {path} ...")
    try:
        content_file = private_repo.get_contents(path)
        raw_data = json.loads(base64.b64decode(content_file.content).decode('utf-8'))
        
        items = config["module"].process(raw_data, path)
        if items:
            # 批量插入
            for i in range(0, len(items), 500):
                supabase.table(config["table_name"]).insert(items[i : i+500]).execute()
            
            # 标记已处理
            supabase.table("processed_files").upsert({
                "file_sha": sha, 
                "file_path": path,
                "engine": config.get("table_name", "unknown").split('_')[0],
                "item_count": len(items)
            }).execute()
            return True
    except Exception as e: 
        print(f"⚠️ 解析失败 {path}: {e}")
    return False

def sync_bank_to_sql(processors_config, full_scan=False):
    """
    双模式同步：
    - full_scan=True: 地毯式扫描整个仓库 (递归遍历)
    - full_scan=False: 只看过去 24h 提交 (快)
    """
    if full_scan:
        # 🔥 [修改点 3] 真正的递归全量扫描逻辑
        print("\n🚜 [全量模式] 正在地毯式扫描 Central-Bank 所有历史文件...")
        try:
            contents = private_repo.get_contents("")
            while contents:
                file_content = contents.pop(0)
                if file_content.type == "dir":
                    contents.extend(private_repo.get_contents(file_content.path))
                elif file_content.name.endswith(".json"):
                    # 找到 JSON，判断属于哪个插件
                    source_key = file_content.path.split('/')[0] # twitter, polymarket...
                    if source_key in processors_config:
                        process_and_upload(file_content.path, file_content.sha, processors_config[source_key])
        except Exception as e:
            print(f"❌ 全量扫描中断: {e}")
            
    else:
        print("\n⚡ [增量模式] 正在检查过去 24h 的提交...")
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        commits = private_repo.get_commits(since=since)
        for commit in commits:
            for f in commit.files:
                if f.filename.endswith('.json'):
                    source_key = f.filename.split('/')[0]
                    if source_key in processors_config:
                        process_and_upload(f.filename, f.sha, processors_config[source_key])

# === 🚀 6. 执行入口 ===
if __name__ == "__main__":
    all_procs = get_all_processors()
    
    # 检查环境变量 FORCE_FULL_SCAN 是否为 true
    is_full_scan = (os.environ.get("FORCE_FULL_SCAN") == "true")
    
    # 1. 同步数据
    sync_bank_to_sql(all_procs, full_scan=is_full_scan)
    
    # 2. 生成战报
    generate_hot_reports(all_procs)
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] ✅ 审计任务完成。")
