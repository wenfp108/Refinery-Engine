import pandas as pd
import hashlib, json, os, requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class SmartFactory:
    def __init__(self, masters_path="masters"):
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()
        self.api_key = os.environ.get("SILICON_FLOW_KEY")
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"

    def _load_masters(self):
        masters = {}
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
        """通用调用接口"""
        if not self.api_key: return "ERROR", "Missing Key"
        payload = {
            "model": model,
            "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_content}],
            "temperature": temperature,
            "max_tokens": 1024
        }
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        try:
            res = requests.post(self.api_url, json=payload, headers=headers, timeout=60).json()
            return "SUCCESS", res['choices'][0]['message']['content']
        except Exception as e:
            return "ERROR", str(e)

    def pre_audit_is_trash(self, row):
        """第一道防线：使用免费模型进行噪音过滤"""
        # 使用免费的 7B 模型
        model = "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B"
        content = str(row.get('eventTitle') or row.get('full_text') or '')
        system_prompt = "你是一个专业的信息过滤器。判断以下信息是否为无价值的噪音（如水贴、广告、纯情绪输出）。如果是噪音，只回答'TRASH'；如果具有分析价值，只回答'VALUE'。"
        
        status, reply = self.call_ai(model, system_prompt, content, temperature=0.1)
        return "TRASH" in reply.upper() if status == "SUCCESS" else False

    def process_and_ship(self, input_raw, vault_path):
        df = pd.read_parquet(input_raw)
        day_str = datetime.now().strftime('%Y%m%d')
        output_file = Path(vault_path) / "instructions" / f"teachings_{day_str}.jsonl"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # 预加载今日已处理的 ID（去重）
        processed_ids = set()
        if output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try: processed_ids.add(json.loads(line).get('ref_id'))
                    except: pass

        rows = df.to_dict('records')
        print(f"🏭 工厂启动: 待处理 {len(rows)} 条。")

        def audit_task(row):
            ref_id = hashlib.sha256(str(row.get('eventTitle','')+row.get('question','')).encode()).hexdigest()
            if ref_id in processed_ids: return None # 基础去重

            # 1. 免费初审 (守门员)
            if self.pre_audit_is_trash(row):
                print(f"🗑️ 过滤噪音: {ref_id[:8]}")
                return None

            # 2. 正式审计 (核心大脑) - 使用收费但顶级的 V3
            results = []
            title = str(row.get('eventTitle') or row.get('full_text') or '未命名')[:50]
            
            # 这里封装一个给大师调用的函数，固定使用 V3 模型
            def ask_v3(sys, usr):
                status, reply = self.call_ai("deepseek-ai/DeepSeek-V3", sys, usr)
                if status == "SUCCESS" and "### Thought" in reply:
                    return reply.split("### Thought")[1].split("### Output")[0].strip(), reply.split("### Output")[1].strip()
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

        # 并发处理
        with ThreadPoolExecutor(max_workers=5) as executor:
            all_batches = list(executor.map(audit_task, rows))

        # 写入
        with open(output_file, 'a', encoding='utf-8') as f:
            for res_list in all_batches:
                if res_list: f.write('\n'.join(res_list) + '\n')

        print("🚀 资产入库完成。")
