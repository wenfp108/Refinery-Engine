import os, json, base64, requests, importlib.util, sys
from datetime import datetime, timedelta, timezone
import pandas as pd
from supabase import create_client
from github import Github

# === 🛡️ 1. 核心配置 ===
PRIVATE_BANK_ID = "wenfp108/Central-Bank" 
GITHUB_TOKEN = os.environ.get("GH_PAT") 
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([GITHUB_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    sys.exit("❌ [审计异常] 环境变量缺失。")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
gh_client = Github(GITHUB_TOKEN)
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

# === ⏱️ 辅助：检查数据新鲜度 (铁面无私版) ===
def get_data_freshness(table_name):
    """
    检查该表最近一条数据的 bj_time 距离现在多久
    返回: (is_fresh, minutes_ago, latest_time_str)
    """
    try:
        # 查最新一条的时间
        res = supabase.table(table_name).select("bj_time").order("bj_time", desc=True).limit(1).execute()
        if not res.data: return (False, 9999, "无数据")
        
        last_time_str = res.data[0]['bj_time']
        if not last_time_str: return (False, 9999, "无时间戳")

        # 转换时间 (处理 ISO 格式)
        try:
            last_time = datetime.fromisoformat(last_time_str.replace('Z', '+00:00'))
        except:
            return (False, 9999, last_time_str)
        
        # 现在的北京时间
        now = datetime.now(timezone(timedelta(hours=8)))
        
        # 统一时区信息
        if last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=timezone(timedelta(hours=8)))
        
        diff = now - last_time
        minutes_ago = int(diff.total_seconds() / 60)
        
        # 🔥 判定标准：65分钟内有新数据算 Fresh (留5分钟buffer) 🔥
        return (minutes_ago <= 65, minutes_ago, last_time.strftime('%H:%M'))
    except Exception as e:
        print(f"⚠️ 新鲜度检查失败 ({table_name}): {e}")
        return (True, 0, "CheckError") # 出错兜底显示

# === 🔥 3. 战报工厂：智能堆叠引擎 ===

def generate_hot_reports(processors_config):
    print("\n🔥 [情报对冲] 正在生成 Markdown 时报...")
    bj_now = datetime.now(timezone(timedelta(hours=8)))
    date_tag = bj_now.strftime('%Y%m%d')
    hour_tag = bj_now.strftime('%H')
    
    md_report = f"# 🚀 Architect's Alpha 情报审计 ({date_tag} {hour_tag}:00)\n\n"
    md_report += "> **机制说明**：全源智能去重 | 无更新源自动折叠\n\n"

    has_content = False

    # 遍历所有插件
    for source_name, config in processors_config.items():
        if hasattr(config["module"], "get_hot_items"):
            try:
                table = config["table_name"]
                
                # 🔥 1. 检查新鲜度 (无差别对待)
                is_fresh, mins_ago, last_update_time = get_data_freshness(table)
                
                # 🔥 2. 睡眠模式 (无新数据) -> 连 Polymarket 也要睡
                if not is_fresh:
                    md_report += f"## 💤 来源：{source_name.upper()} (上次更新: {last_update_time})\n"
                    # 仅显示一行提示，不再占版面
                    md_report += f"> *距上次更新已过 {int(mins_ago/60)} 小时，暂无新数据。*\n\n"
                    continue # 跳过后续渲染

                # 🔥 3. 唤醒模式 (有新数据)
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
                        
                        # 兼容所有插件的 URL
                        url = item.get('url') or item.get('tweet_url') or '#'
                        
                        md_report += f"| **{score:,}** | {source} | {text} | [查看]({url}) |\n"
                    md_report += "\n"
            except Exception as e:
                print(f"⚠️ {source_name} 渲染异常: {e}")

    # 如果所有源都在睡觉，加个提示
    if not has_content:
        md_report += "\n\n**🛑 本轮扫描全域静默，请查阅历史归档。**"

    # 上传 Github
    latest_path = "reports/latest_brief.md"
    archive_path = f"reports/hourly/{date_tag}_{hour_tag}.md"
    
    for path in [latest_path, archive_path]:
        try:
            try:
                old = private_repo.get_contents(path)
                private_repo.update_file(old.path, f"📊 Update: {hour_tag}h", md_report, old.sha)
            except:
                private_repo.create_file(path, f"🚀 New: {hour_tag}h", md_report)
        except Exception as e: print(f"❌ 写入 {path} 失败: {e}")

# === 🏦 5. 搬运逻辑 (保持原样) ===
def process_and_upload(path, sha, config):
    check = supabase.table("processed_files").select("file_sha").eq("file_sha", sha).execute()
    if check.data: return False 
    try:
        content_file = private_repo.get_contents(path)
        raw_data = json.loads(base64.b64decode(content_file.content).decode('utf-8'))
        items = config["module"].process(raw_data, path)
        if items:
            for i in range(0, len(items), 500):
                supabase.table(config["table_name"]).insert(items[i : i+500]).execute()
            supabase.table("processed_files").upsert({"file_sha": sha, "file_path": path}).execute()
            return True
    except Exception as e: print(f"⚠️ {path} 解析异常: {e}")
    return False

def sync_bank_to_sql(processors_config):
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
    
    # 1. 同步数据
    sync_bank_to_sql(all_procs)
    
    # 2. 生成战报 (每小时都跑，能不能显示全看是否有新数据)
    generate_hot_reports(all_procs)
    
    # 3. 每日清理 (可选)
    if datetime.now(timezone.utc).hour == 20: 
         # perform_grand_harvest(all_procs)
         pass
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] ✅ 审计任务完成。")
