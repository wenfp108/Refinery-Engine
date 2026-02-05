import pandas as pd
import hashlib, json, os, requests, importlib.util
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class UniversalFactory: # ✅ 统一改回这个名字，修复 ImportError
    def __init__(self, masters_path="masters"):
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()
        self.api_key = os.environ.get("SILICON_FLOW_KEY")
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        if not self.api_key:
            print("⚠️ [警告] 未检测到 SILICON_FLOW_KEY，请检查 GitHub Secrets。")

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

    def call_ai(self, model, system_prompt, user_content, temperature=0.7):
        """通用 API 调用，支持代金券自动抵扣"""
        if not self.api_key: return "ERROR", "Missing API Key"
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            "temperature": temperature,
            "max_tokens": 1024
        }
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        try:
            response = requests.post(self.api_url, json=payload, headers=headers, timeout=60)
            res_json = response.json()
            return "SUCCESS", res_json['choices'][0]['message']['content']
        except Exception as e:
            return "ERROR", str(e)

    def pre_audit_is_trash(self, row):
        """第一道防线：使用免费模型过滤噪音"""
        # 使用硅基流动免费的 7B 模型
        model = "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B"
        content = str(row.get('eventTitle') or row.get('full_text') or '无标题信号')
        system_prompt = "你是一个信息过滤器。只判断信息是否有分析价值。如果是无意义噪音（水贴、广告、纯情绪），只回答'TRASH'；如果有分析价值，只回答'VALUE'。"
        
        status, reply = self.call_ai(model, system_prompt, content, temperature=0.1)
        # 如果报错，默认认为有价值，防止误杀
        if status == "ERROR": return False 
        return "TRASH" in reply.upper()

    def process_and_ship(self, input_raw, vault_path):
        df = pd.read_parquet(input_raw)
        day_str = datetime.now().strftime('%Y%m%d')
        output_file = Path(vault_path) / "instructions" / f"teachings_{day_str}.jsonl"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # 预加载今日已处理的 ID（去重，省钱逻辑）
        processed_ids = set()
        if output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try: processed_ids.add(json.loads(line).get('ref_id'))
                    except: pass

        rows = df.to_dict('records')
        print(f"🏭 工厂任务启动: 处理 {len(rows)} 条，已过滤 {len(processed_ids)} 条重复。")

        def audit_task(row):
            # 生成唯一标识
            ref_id = hashlib.sha256(str(row.get('eventTitle','')+row.get('question','')).encode()).hexdigest()
            if ref_id in processed_ids: return None

            # 1. 免费初审 (省钱策略)
            if self.pre_audit_is_trash(row):
                return None

            # 2. 核心审计 (大师议会 - 使用顶级 V3)
            results = []
            title = str(row.get('eventTitle') or row.get('full_text') or '未命名')[:50]
            
            def ask_v3(sys, usr):
                status, reply = self.call_ai("deepseek-ai/DeepSeek-V3", sys, usr)
                if status == "SUCCESS" and "### Thought" in reply:
                    parts = reply.split("### Output")
                    thought = parts[0].replace("### Thought", "").strip()
                    output = parts[1].strip() if len(parts) > 1 else reply
                    return thought, output
                return "综合分析", reply

            for name, mod in self.masters.items():
                try:
                    t, o = mod.audit(row, ask_v3)
                    if t and o:
                        results.append(json.dumps({
                            "ref_id": ref_id, "master": name, 
                            "instruction": f"请分析: {title}", 
                            "thought": t, "output": o
                        }, ensure_ascii=False))
                except: continue
            return results

        # 🚀 开启并发执行 (提升 5 倍速度)
        with ThreadPoolExecutor(max_workers=5) as executor:
            all_batches = list(executor.map(audit_task, rows))

        # 资产写入
        count = 0
        with open(output_file, 'a', encoding='utf-8') as f:
            for res_list in all_batches:
                if res_list:
                    f.write('\n'.join(res_list) + '\n')
                    count += 1
        
        print(f"🚀 任务结束：本轮产出 {count} 条顶级认知资产。")
