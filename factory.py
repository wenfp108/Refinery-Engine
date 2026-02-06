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
        self.memory = {} 

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
        """🧠 跨时区记忆同步：锁定今日已审计的哈希，省钱核心"""
        day_str = datetime.now().strftime('%Y%m%d')
        instructions_dir = vault_path / "instructions"
        if not instructions_dir.exists(): return set()
        
        day_processed_ids = set()
        print(f"🧐 正在加载今日全天（2小时步进）记忆...")
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
        """🌟 严格保留你的原装权重 50/60/30/80"""
        try:
            supabase = create_client(self.supabase_url, self.supabase_key)
            print("💎 启动 2 小时一度精锐筛选...")

            # === 1. GitHub 信号独立处理 (保底 20 条) ===
            print("💎 正在获取 GitHub 信号...")
            # limit(50) 是为了多抓取一些生肉，防止去重后不够 20 条
            github_raw = supabase.table("raw_signals").select("*").eq("signal_type", "github").order("created_at", desc=True).limit(50).execute().data or []
            
            unique_github = {}
            for r in github_raw:
                # GitHub 专属去重键：repo_name
                name = r.get('repo_name')
                if name and name not in unique_github:
                    unique_github[name] = r
            
            # 独立截断：确保 GitHub 稳拿前 20
            github_picks = list(unique_github.values())[:20]
            print(f"✅ GitHub 处理完成：去重后获 {len(github_picks)} 条")

            # === 2. Paper 信号独立处理 (保底 30 条) ===
            print("💎 正在获取 Paper 信号...")
            # limit(50) 同理，为了保底
            paper_raw = supabase.table("raw_signals").select("*").eq("signal_type", "papers").order("created_at", desc=True).limit(50).execute().data or []
            
            unique_paper = {}
            for r in paper_raw:
                # Paper 专属去重键：title
                title = r.get('title')
                if title and title not in unique_paper:
                    unique_paper[title] = r
            
            # 独立截断：确保 Paper 稳拿前 30
            paper_picks = list(unique_paper.values())[:30]
            print(f"✅ Paper 处理完成：去重后获 {len(paper_picks)} 条")

            # === 3. 最终汇总 (仅用于后续循环) ===
            # 这里合并的是已经处理干净的成品列表，不会再有冲突
            rare_picks = github_picks + paper_picks

            # === 4. Twitter (VIP 权重) - 保持原样 ===
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

            # === 5. Reddit (Vibe 权重) - 保持原样 ===
            rd_raw = supabase.table("raw_signals").select("*").eq("signal_type", "reddit").order("created_at", desc=True).limit(500).execute().data or []
            unique_rd = {r.get('url'): r for r in rd_raw if r.get('url')}
            def score_reddit(row): return (row.get('score') or 0) * (1 + abs(float(row.get('vibe') or 0)))
            rd_picks = sorted(unique_rd.values(), key=score_reddit, reverse=True)[:30]

            # === 6. Polymarket (Tail_Risk 权重) - 保持原样 ===
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
        
        # 严格对齐格式
        parts = [f"【Source: {source.upper()}】"]
        if source == 'github':
            parts.append(f"项目: {row.get('repo_name')} | Stars: {row.get('stars')} | Topics: {row.get('topics')}")
            parts.append(f"描述: {row.get('full_text') or '新项目发布'} | Link: {row.get('url')}")
        elif source == 'papers':
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
        
        # 核心去重：如果今天审过，直接跳过，不花 API 钱
        if ref_id in processed_ids: return []

        results = []
        def ask_v3(s, u):
            st, r = self.call_ai(self.v3_model, s, u)
            if st == "SUCCESS" and "### Output" in r:
                return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
            return "Audit", r

        for name, mod in self.masters.items():
            prev_opinion = self.memory.get(topic_id, {}).get(name)
            drift_context = f"\n\n[历史记忆]：此前观点：'{prev_opinion}'。数据变动若触发逻辑反转，请在 Output 开头标记 [DRIFT_DETECTED]。" if prev_opinion else ""
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
            except: continue
        return results

    def process_and_ship(self, vault_path="vault"):
        self.vault_path = Path(vault_path)
        (self.vault_path / "instructions").mkdir(parents=True, exist_ok=True)
        
        # 1. 加载今日全天去重 ID
        processed_ids = self.build_day_memory(self.vault_path)
        
        now = datetime.now()
        day_str = now.strftime('%Y%m%d')
        hour_str = now.strftime('%H')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}_{hour_str}.jsonl"

        # 2. 筛选
        signals = self.fetch_elite_signals()
        if not signals: return

        # 3. 审计并实时锁定 ID
        batch_size = 50
        for i in range(0, len(signals), batch_size):
            chunk = signals[i : i + batch_size]
            with ThreadPoolExecutor(max_workers=20) as executor:
                res = list(executor.map(lambda r: self.audit_process(r, processed_ids), chunk))
            
            added = []
            for r_list in res:
                if r_list:
                    added.extend(r_list)
                    # 实时存入，防止同一批次内由于 Supabase 延迟导致的重复
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
        """防御型推送：解决身份未知、未提交更改以及远程拒绝问题"""
        if not self.vault_path: return
        cwd = self.vault_path
        
        # === 🛡️ 新增：自愈逻辑 ===
        # 检查是否存在僵尸 rebase 锁，如果有，先杀掉
        rebase_dir = cwd / ".git" / "rebase-merge"
        if rebase_dir.exists():
            print("🚑 检测到僵尸 Rebase 锁，正在执行战地急救...")
            subprocess.run(["git", "rebase", "--abort"], cwd=cwd)
            if rebase_dir.exists(): # 如果 abort 失败，直接物理删除
                import shutil
                shutil.rmtree(rebase_dir)
        # =======================

        # 1. 强制注入身份
        subprocess.run(["git", "config", "user.email", "bot@factory.com"], cwd=cwd)
        subprocess.run(["git", "config", "user.name", "Cognitive Bot"], cwd=cwd)
        # 解决 pull 时的 rebase 策略警告
        subprocess.run(["git", "config", "pull.rebase", "true"], cwd=cwd)

        # 2. 【顺序调整】先 add 和 commit，把你的 1000 多条数据存进本地仓库
        subprocess.run(["git", "add", "."], cwd=cwd)
        
        # 检查是否有东西可以 commit
        diff_status = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd)
        if diff_status.returncode == 0:
            print("💤 没有发现新资产，跳过同步。")
            return

        # 3. 执行 Commit
        commit_msg = f"🧠 Cognitive Audit: {datetime.now().strftime('%H:%M:%S')}"
        subprocess.run(["git", "commit", "-m", commit_msg], cwd=cwd)

        # 4. 【同步远程】此时再 pull --rebase，Git 就能顺畅地把远程改动接在你的 commit 之后
        print("🔄 正在通过 rebase 同步远程仓库...")
        subprocess.run(["git", "pull", "origin", "main", "--rebase"], cwd=cwd)

        # 5. 最终推送
        push_res = subprocess.run(["git", "push", "origin", "main"], cwd=cwd, capture_output=True, text=True)
        
        if push_res.returncode == 0:
            print("🚀 认知资产已成功同步至中央银行。")
        else:
            print(f"❌ 最终推送失败: {push_res.stderr}")

if __name__ == "__main__":
    factory = UniversalFactory()
    factory.process_and_ship()
