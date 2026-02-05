import pandas as pd
import hashlib
import json
import os
import importlib.util
import requests  # ✅ 必须安装: pip install requests
from pathlib import Path
from datetime import datetime

class UniversalFactory:
    def __init__(self, masters_path="masters"):
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()
        # 从环境变量读取硅基流动 Key
        self.api_key = os.environ.get("SILICON_FLOW_KEY")

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
                if hasattr(module, 'audit'):
                    masters[name] = module
                    print(f"✅ [加载成功] 认知插件: {name}")
            except Exception as e: print(f"❌ [加载失败] {file_path.name}: {e}")
        return masters

    def ask_llm(self, system_prompt, user_content):
        """统一调用硅基流动 DeepSeek-V3 API"""
        if not self.api_key:
            return "API_ERROR", "缺少 SILICON_FLOW_KEY 环境变量"
            
        url = "https://api.siliconflow.cn/v1/chat/completions"
        payload = {
            "model": "deepseek-ai/DeepSeek-V3", 
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            "temperature": 0.7,
            "max_tokens": 1024
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=60)
            res_json = response.json()
            full_reply = res_json['choices'][0]['message']['content']
            
            # 尝试解析 Thought 和 Output 格式
            if "### Thought" in full_reply and "### Output" in full_reply:
                thought = full_reply.split("### Thought")[1].split("### Output")[0].strip()
                output = full_reply.split("### Output")[1].strip()
            else:
                thought = "分析已合并"
                output = full_reply
            return thought, output
        except Exception as e:
            return "API_TIMEOUT", f"请求失败: {str(e)}"

    def generate_ref_id(self, row_dict):
        p1 = str(row_dict.get('eventTitle') or '')
        p2 = str(row_dict.get('question') or '')
        content = f"{p1}{p2}"
        if not p1 and not p2:
            content = json.dumps(row_dict, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()

    def process_and_ship(self, input_raw, vault_path, batch_size=2000):
        input_path = Path(input_raw)
        vault_dir = Path(vault_path)
        if not input_path.exists(): return

        df = pd.read_parquet(input_path)
        current_month = datetime.now().strftime('%Y%m') 
        output_file = vault_dir / "instructions" / f"teachings_{current_month}.jsonl"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        rows = df.to_dict('records')
        buffer = []

        with open(output_file, 'a', encoding='utf-8') as f:
            for row_dict in rows:
                ref_id = self.generate_ref_id(row_dict)
                raw_title = row_dict.get('eventTitle') or row_dict.get('full_text') or '未命名信号'
                event_title = str(raw_title)[:50].replace('\n', ' ')

                for master_name, master_mod in self.masters.items():
                    try:
                        # ✅ 核心变动：将 ask_llm 方法传给大师
                        thought, output = master_mod.audit(row_dict, self.ask_llm)

                        if thought and output:
                            entry = {
                                "ref_id": ref_id,
                                "master": master_name,
                                "instruction": f"请分析事件: {event_title}",
                                "thought": thought,
                                "output": output
                            }
                            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                    except Exception: continue

        print(f"🚀 [发货完成] 资产注入: {output_file}")
