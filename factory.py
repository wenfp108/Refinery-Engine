import pandas as pd
import hashlib, json, os, requests, importlib.util, subprocess, time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client

class UniversalFactory:
    def __init__(self, masters_path="masters"):
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()
        self.api_key = os.environ.get("SILICON_FLOW_KEY")
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.vault_path = None
        
        self.v3_model = "deepseek-ai/DeepSeek-V3"
        self.free_model = "Qwen/Qwen2.5-7B-Instruct"

    def _load_masters(self):
        masters = {}
        if not self.masters_path.exists(): return masters
        for file_path in self.masters_path.glob("*.py"):
            if file_path.name.startswith("__"): continue
            try:
                name = file_path.stem
                spec = importlib.util.spec_from_file_location(name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                if hasattr(module, 'audit'): masters[name] = module
            except: pass
        return masters

    def fetch_best_signals(self, limit=300):
        """尝试从 SQL 获取，如果表名不对会报错，由主函数捕获"""
        print(f"📡 尝试连接 SQL 筛选前 {limit} 条精华...")
        supabase = create_client(self.supabase_url, self.supabase_key)
        # ⚠️ 注意：这里假设表名叫 raw_signals，如果你的表叫 items 或其他名字，请修改这里
        response = supabase.table("raw_signals") \
            .select("*") \
            .order("created_at", desc=True) \
            .limit(limit) \
            .execute()
        return response.data

    def call_ai(self, model, sys, usr):
        if not self.api_key: return "ERROR", "Missing Key"
        payload = {
            "model": model, "messages": [{"role": "system", "content": sys}, {"role": "user", "content": usr}],
            "temperature": 0.7, "max_tokens": 1024
        }
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        try:
            res = requests.post(self.api_url, json=payload, headers=headers, timeout=45).json()
            return "SUCCESS", res['choices'][0]['message']['content']
        except: return "ERROR", "Timeout"

    def git_push_assets(self):
        if not self.vault_path: return
        try:
            cwd = self.vault_path
            subprocess.run(["git", "add", "."], cwd=cwd, check=True)
            status = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd)
            if status.returncode != 0:
                print("📦 [分批同步] 资产入库中...")
                subprocess.run(["git", "commit", "-m", f"🧠 Batch Sync: {datetime.now().strftime('%H:%M:%S')}"], cwd=cwd, check=True)
                subprocess.run(["git", "push"], cwd=cwd, check=True)
        except Exception as e: print(f"⚠️ Git同步警告: {e}")

    def audit_process(self, row):
        # 兼容不同数据源的字段名
        content = str(row.get('full_text') or row.get('eventTitle') or row.get('text') or '')
        if not content: return []

        ref_id = hashlib.sha256(content.encode()).hexdigest()
        
        # 1. 免费打分 (Scout)
        scout_sys = "你是一个高价值信息筛选器。给以下内容打分(0-100)。只回答数字。"
        _, score_reply = self.call_ai(self.free_model, scout_sys, content[:500])
        
        try: score = int(''.join(filter(str.isdigit, score_reply)))
        except: score = 50

        results = []
        title = content[:50]
        
        # 精华：V3 全量
        if score > 80:
            def ask_v3(s, u):
                st, r = self.call_ai(self.v3_model, s, u)
                if st == "SUCCESS" and "### Output" in r:
                    return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
                return "综合研判", r
            
            for name, mod in self.masters.items():
                try:
                    t, o = mod.audit(row, ask_v3)
                    if t and o: results.append(json.dumps({"ref_id":ref_id, "master":name, "instruction":f"研判: {title}", "thought":t, "output":o}, ensure_ascii=False))
                except: continue
        
        # 普通：免费快评
        elif score > 50:
            st, r = self.call_ai(self.free_model, "请用一句话提取关键价值", content[:500])
            if st == "SUCCESS":
                results.append(json.dumps({"ref_id":ref_id, "master":"system", "instruction":f"快评: {title}", "thought":"快速扫描", "output":r}, ensure_ascii=False))

        return results

    def process_and_ship(self, input_raw, vault_path):
        self.vault_path = Path(vault_path)
        signals = []
        
        # 🛡️【双保险逻辑】先试 SQL，不行就读文件
        try:
            signals = self.fetch_best_signals(limit=300)
            print(f"✅ SQL 连接成功，获取到 {len(signals)} 条数据。")
        except Exception as e:
            print(f"⚠️ SQL 表名错误或连接失败: {e}")
            print(f"🔄 自动切换至本地文件模式 (读取 {input_raw})...")
            try:
                # 降级读取本地 Parquet
                df = pd.read_parquet(input_raw)
                # 模拟 limit=300
                signals = df.head(300).to_dict('records')
                print(f"✅ 本地文件读取成功，处理前 {len(signals)} 条。")
            except Exception as file_e:
                print(f"❌ 严重错误: 本地文件也无法读取: {file_e}")
                return

        day_str = datetime.now().strftime('%Y%m%d')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}.jsonl"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        batch_size = 50
        print(f"🚀 工厂开工！当前模式: {'SQL精选' if 'raw_signals' in str(signals) else '本地文件'}")

        for i in range(0, len(signals), batch_size):
            batch_rows = signals[i : i + batch_size]
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                batch_results = list(executor.map(self.audit_process, batch_rows))
            
            batch_added = 0
            with open(output_file, 'a', encoding='utf-8') as f:
                for res_list in batch_results:
                    if res_list:
                        f.write('\n'.join(res_list) + '\n')
                        batch_added += 1
            
            print(f"✨ 进度: {i+len(batch_rows)}/{len(signals)}。本批次产出 {batch_added} 条。")
            self.git_push_assets()

        print("🏁 任务完成。")
