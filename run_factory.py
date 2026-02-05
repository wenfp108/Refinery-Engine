import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from supabase import create_client
from factory import UniversalFactory  # 导入你的通用工厂类

# === ⚙️ 配置区 ===
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 你的中央银行在 GitHub Action 里的相对路径 (根据 workflow 配置)
VAULT_PATH = "../vault"

# 你所有的情报源表名 (需要与 processors 里的 TABLE_NAME 一致)
TARGET_TABLES = [
    "polymarket_logs",
    "twitter_logs",
    "reddit_logs",
    "github_logs",
    "papers_logs"
]

def fetch_fresh_data(table_name, minutes=70):
    """
    从指定表捞取最近 N 分钟的数据
    (70分钟是为了稍微覆盖整点，防止边缘数据遗漏)
    """
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # 计算时间阈值 (UTC 时间，因为 Supabase 内部通常存 UTC 或带时区的 ISO)
        # 注意：这里假设你的 bj_time 是 ISO 格式字符串
        # 为了保险，我们用当前时间减去 70 分钟的 ISO 字符串进行字符串比较
        # (只要格式是标准的 ISO 8601，字符串比较就是有效的)
        cutoff_time = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        
        # 兼容性处理：如果你的 bj_time 是 +08:00，这里最好也转换一下
        # 简单起见，这里直接利用 Supabase 的过滤器
        
        print(f"🎣 [{table_name}] 正在扫描新数据...")
        
        # 限制单次最大获取 1000 条，防止内存爆
        res = supabase.table(table_name)\
            .select("*")\
            .gt("bj_time", cutoff_time)\
            .limit(1000)\
            .execute()
            
        data = res.data
        if data:
            print(f"   ✅ 捕获 {len(data)} 条信号")
            return data
        else:
            print(f"   💤 无新增信号")
            return []
            
    except Exception as e:
        print(f"   ⚠️ [{table_name}] 读取失败: {e}")
        return []

def main():
    print(f"🚀 [Cognitive Factory] 启动时间: {datetime.now().isoformat()}")
    
    all_signals = []
    
    # 1. 遍历所有源，收集新鲜原料
    for table in TARGET_TABLES:
        rows = fetch_fresh_data(table)
        if rows:
            all_signals.extend(rows)
            
    if not all_signals:
        print("📭 本轮巡检未发现任何新数据，工厂休眠。")
        return

    print(f"📦 原料准备完毕，共计 {len(all_signals)} 条混合信号。")

    # 2. 转换为 DataFrame 并保存为临时 Parquet
    # (Factory 只吃 Parquet，这样可以保持接口统一)
    df = pd.DataFrame(all_signals)
    temp_file = "temp_run_batch.parquet"
    
    # 兼容性：确保 numeric 字段是数字类型，防止报错
    for col in ['volume', 'liquidity', 'vol24h', 'day_change', 'stars', 'citations']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    df.to_parquet(temp_file)

    # 3. 唤醒大师，开工
    # masters_path="masters" 对应 workflow 里复制过来的插件目录
    try:
        factory = UniversalFactory(masters_path="masters")
        
        print("🏭 流水线全速运转中...")
        factory.process_and_ship(
            input_raw=temp_file, 
            vault_path=VAULT_PATH
        )
        
    except Exception as e:
        print(f"❌ 工厂运行严重错误: {e}")
        
    finally:
        # 4. 清理现场 (焚烧临时文件)
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print("🧹 临时文件已清理。")

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ [错误] 环境变量缺失 (SUPABASE_URL/KEY)")
    else:
        main()
