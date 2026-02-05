import pandas as pd
import hashlib, json, os, requests, subprocess, time, sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client
import importlib.util

class UniversalFactory:
    def __init__(self, masters_path="masters"):
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()
        self.api_key = os.environ.get("SILICON_FLOW_KEY") 
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.v3_model = "deepseek-ai/DeepSeek-V3"
        self.vault_path = None
        self.memory = {} # 🧠 跨时区认知记忆库

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
                print(f"✅ 已加载 Master: {name}")
            except: pass
        return masters

    def build_day_memory(self, vault_path):
        """🧠 跨时区记忆同步：扫描今日全时段文件，确保不重复、能改口"""
        day_str = datetime.now().strftime('%Y%m%d')
        instructions_dir = vault_path / "instructions"
        if not instructions_dir.exists(): return set()
        
        day_processed_ids = set()
        print(f"🧐 正在加载今日全天记忆...")
        for f in instructions_dir.glob(f"teachings_{day_str}_*.jsonl"):
            try:
                with open(f, 'r', encoding='utf-8') as f_in:
                    for line in f_in:
                        try:
                            data = json.loads(line)
                            tid, m, rid = data.get('topic_id'), data.get('master'), data.get('ref_id')
                            if tid and m:
                                if tid not in self.memory: self.memory[tid] = {}
                                self.memory[tid][m] = data.get('output', "")
                            if rid: day_processed_ids.add(rid)
                        except: continue
            except: pass
        print(f"✅ 记忆构建：锁定 {len(day_processed_ids)} 个历史哈希")
        return day_processed_ids

    def fetch_elite_signals(self):
        """🌟 核心逻辑：精锐席位筛选 (严格保留 50/60/30/80 原始配额)"""
        try:
            supabase = create_client(self.supabase_url, self.supabase_key)
            print("💎 启动精锐筛选 (目标: ~220 条)...")

            # 1. GitHub & Paper (Limit 50)
            rare_raw = supabase.table("raw_signals").select("*").or_("signal_type.eq.github,signal_type.eq.paper").order("created_at", desc=True).limit(50).execute().data or []
            unique_rare = {}
            for r in rare_raw:
                k = r.get('repo_name') or r.get('title')
                if k and k not in unique_rare: unique_rare[k] = r
            rare_picks = list(unique_rare.values())

            # 2. Twitter (Limit 60 | VIP 权重)
            tw_raw = supabase.table("raw_signals").select("*").eq("signal_type", "twitter").order("created_at", desc=True).limit(500).execute().data or []
            vip_list = ['Karpathy', 'Musk', 'Vitalik', 'LeCun', 'Dalio', 'Naval', 'Sama', 'PaulG']
            def score_twitter(row):
                rt, bm, like = row.get('retweets',0), row.get('bookmarks',0), row.get('likes',0)
                user = str(row.get('user_name', '')).lower()
                score = (rt * 5) + (bm * 10) + like
                if any(v.lower() in user for v in vip_list):
                    score += 10000 if (rt > 10 or like > 50) else 500
                return score
            for r in tw_raw: r['_rank'] = score_twitter(r)
            tw_picks = sorted(tw_raw, key=lambda x:x['_rank'], reverse=True)[:60]

            # 3. Reddit (Limit 30 | Vibe 权重)
            rd_raw = supabase.table("raw_signals").select("*").eq("signal_type", "reddit").order("created_at", desc=True).limit(500).execute().data or []
            unique_rd = {r.get('url'): r for r in rd_raw if r.get('url')}
            def score_reddit(row): return (row.get('score') or 0) * (1 + abs(float(row.get('vibe') or 0)))
            rd_picks = sorted(unique_rd.values(), key=score_reddit, reverse=True)[:30]

            # 4. Polymarket (Limit 80 | Tail_Risk 权重)
            poly_raw = supabase.table("raw_signals").select("*").eq("signal_type", "polymarket").order("created_at", desc=True).limit(800).execute().data or []
            unique_poly = {}
            for p in poly_raw:
                raw = p.get('raw_json')
                if isinstance(raw, str):
                    try: raw = json.loads(raw)
                    except: raw = {}
                p['_parsed'] = raw
                slug = p.get('slug') or raw.get('slug')
                if slug:
                    curr_liq = float(p.get('liquidity') or 0)
                    if slug not in unique_poly or curr_liq > float(unique_poly[slug].get('liquidity',0)):
                        unique_poly[slug] = p
            def score_poly(row):
                raw, liq = row['_parsed'], float(row.get('liquidity') or 0)
                if 'TAIL_RISK' in raw.get('strategy_tags', []): return 10000000 + liq
                if any(x in str(row.get('category','')).upper() for x in ['ECONOMY', 'TECH']): return 5000000 + liq
                return 1000000 + liq
            poly_picks = sorted(unique_poly.values(), key=score_poly, reverse=True)[:80]

            return rare_picks + tw_picks + rd_picks + poly_picks
        except Exception as e:
            print(f"⚠️ 筛选异常: {e}"); return []

    def audit_process(self, row, processed_ids):
        topic_id = row.get('url') or row.get('slug') or row.get('repo_name') or "unknown"
        source = row.get('signal_type', 'unknown').lower()
        
        # 严格对齐你的格式逻辑
        parts = [f"【Source: {source.upper()}】"]
        if source == 'github':
            parts.append(f"项目: {row.get('repo_name')} | Stars: {row.get('stars')} | Topics: {row.get('topics')}")
            parts.append(f"描述: {row.get('full_text') or '新项目发布'} | Link: {row.get('url')}")
        elif source == 'paper':
            parts.append(f"论文: {row.get('title')} | 期刊: {row.get('journal')}")
            parts.append(f"引用: {row.get('citations')} | 摘要: {row.get('full_text')}")
        elif source in ['twitter', 'reddit']:
            parts.append(f"用户: {row.get('user_name') or row.get('subreddit')} | Score: {row.get('_rank',0)}")
            parts.append(f"内容: {row.get('full_text') or row.get('title')}")
        else: # Polymarket
            raw = row.get('_parsed') or row.get('raw_json') or {}
            parts.append(f"预测: {row.get('title')} | 问题: {row.get('question')}")
            parts.append(f"价格: {row.get('prices') or raw.get('outcome_prices')} | 流动性: ${raw.get('liquidity')}")

        content = "\n".join(parts)
        ref_id = hashlib.sha256(content.encode()).hexdigest()
        
        if ref_id in processed_ids: return []

        results = []
        def ask_v3(s, u):
            st, r = self.call_ai(self.v3_model, s, u)
            if st == "SUCCESS" and "### Output" in r:
                return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
            return "Audit", r

        for name, mod in self.masters.items():
            prev_opinion = self.memory.get(topic_id, {}).get(name)
            drift_context = f"\n\n[历史记忆]：你此前观点：'{prev_opinion}'。若数据变动触发逻辑反转，请在 Output 开头标记 [DRIFT_DETECTED]。" if prev_opinion else ""
            try:
                if hasattr(mod, 'audit'):
                    row['_drift_context'] = drift_context
                    row['full_text_formatted'] = content
                    t, o = mod.audit(row, ask_v3)
                    if t and o:
                        results.append(json.dumps({
                            "ref_id": ref_id, "topic_id": topic_id, "master": name,
                            "drift": "[DRIFT_DETECTED]" in o,
                            "source": source, "thought": t, "output": o
                        }, ensure_ascii=False))
                        print(f"💡 [{name}] {'🔄 改口' if '[DRIFT_DETECTED]' in o else '分析'}: {topic_id[:30]}...")
            except: continue
        return results

    def process_and_ship(self, vault_path="vault"):
        self.vault_path = Path(vault_path)
        (self.vault_path / "instructions").mkdir(parents=True, exist_ok=True)
        
        # 1. 跨时区去重锁：加载今日所有已处理 ID
        processed_ids = self.build_day_memory(self.vault_path)
        
        now = datetime.now()
        day_str = now.strftime('%Y%m%d')
        hour_str = now.strftime('%H')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}_{hour_str}.jsonl"

        # 2. 严格按 220 条左右进货
        signals = self.fetch_elite_signals()
        if not signals: return

        # 3. 处理并实时更新去重库
        batch_size = 50
        for i in range(0, len(signals), batch_size):
            chunk = signals[i : i + batch_size]
            with ThreadPoolExecutor(max_workers=20) as executor:
                res = list(executor.map(lambda r: self.audit_process(r, processed_ids), chunk))
            
            added = []
            for r_list in res:
                if r_list:
                    added.extend(r_list)
                    # 关键修复：处理完立刻把 ID 锁死，防止本轮内重复录入
                    for r_json in r_list: processed_ids.add(json.loads(r_json).get('ref_id'))
            
            if added:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write('\n'.join(added) + '\n')
                self.git_push_assets()

    def call_ai(self, model, sys_prompt, usr_prompt):
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload = {"model": model, "messages": [{"role": "system", "content": sys_prompt}, {"role": "user", "content": usr_prompt}], "temperature": 0.7}
        try:
            res = requests.post(self.api_url, json=payload, headers=headers, timeout=60).json()
            return "SUCCESS", res['choices'][0]['message']['content']
        except: return "ERROR", "AI_FAIL"

    def git_push_assets(self):
        if not self.vault_path: return
        cwd = self.vault_path
        subprocess.run(["git", "pull", "origin", "main", "--rebase"], cwd=cwd, check=False)
        subprocess.run(["git", "add", "."], cwd=cwd, check=False)
        if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd).returncode != 0:
            msg = f"🧠 Cognitive Audit: {datetime.now().strftime('%H:%M:%S')}"
            subprocess.run(["git", "commit", "-m", msg], cwd=cwd, check=False)
            subprocess.run(["git", "push", "origin", "main"], cwd=cwd, check=False)

if __name__ == "__main__":
    factory = UniversalFactory()
    factory.process_and_ship()
