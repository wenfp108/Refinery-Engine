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
        # API 与 数据库配置
        self.api_key = os.environ.get("SILICON_FLOW_KEY") 
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.vault_path = None
        
        # 🛡️ 校验配置
        if not all([self.api_key, self.supabase_url, self.supabase_key]):
            print("❌ [Factory] 启动失败: 环境变量缺失 (SILICON_FLOW_KEY, SUPABASE_URL, SUPABASE_KEY)")
            # 兼容本地测试，不强制退出，但在 Action 里会报错
        
        # 🤖 模型设定：全员 V3
        self.v3_model = "deepseek-ai/DeepSeek-V3"

    def _load_masters(self):
        masters = {}
        if not self.masters_path.exists(): 
            try: self.masters_path.mkdir(exist_ok=True)
            except: pass
            return masters
            
        for file_path in self.masters_path.glob("*.py"):
            if file_path.name.startswith("__"): continue
            try:
                name = file_path.stem
                spec = importlib.util.spec_from_file_location(name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                if hasattr(module, 'audit'): masters[name] = module
                print(f"✅ 已加载 Master: {name}")
            except Exception as e:
                print(f"⚠️ Master {file_path.name} 加载失败: {e}")
        return masters

    def configure_git(self):
        if not self.vault_path: return
        if not self.vault_path.exists():
            self.vault_path.mkdir(parents=True, exist_ok=True)
            
        subprocess.run(["git", "config", "--global", "user.email", "bot@factory.com"], check=False)
        subprocess.run(["git", "config", "--global", "user.name", "Cognitive Bot"], check=False)

    def fetch_elite_signals(self):
        """
        🌟 核心逻辑：精锐席位筛选 (220条)
        """
        try:
            supabase = create_client(self.supabase_url, self.supabase_key)
            print("💎 启动精锐筛选 (目标: ~220 条 | 启用严格去重)...")

            # 1. GitHub & Paper
            rare_raw = supabase.table("raw_signals") \
                .select("*") \
                .or_("signal_type.eq.github,signal_type.eq.paper") \
                .order("created_at", desc=True) \
                .limit(50).execute().data or []
            
            unique_rare = {}
            for r in rare_raw:
                k = r.get('repo_name') or r.get('title')
                if k and k not in unique_rare: unique_rare[k] = r
            rare_picks = list(unique_rare.values())
            print(f"🔹 稀缺源: {len(rare_picks)} 条")

            # 2. Twitter
            tw_raw = supabase.table("raw_signals").select("*").eq("signal_type", "twitter").order("created_at", desc=True).limit(500).execute().data or []
            vip_list = ['Karpathy', 'Musk', 'Vitalik', 'LeCun', 'Dalio', 'Naval', 'Sama', 'PaulG']
            
            def score_twitter(row):
                rt = row.get('retweets') or 0
                bm = row.get('bookmarks') or 0
                like = row.get('likes') or 0
                user = str(row.get('user_name', '')).lower()
                score = (rt * 5) + (bm * 10) + like
                if rt > 10000: score += 5000 
                if any(v.lower() in user for v in vip_list):
                    if rt > 10 or like > 50: score += 10000
                    else: score += 500
                return score

            for r in tw_raw: r['_rank'] = score_twitter(r)
            tw_picks = sorted(tw_raw, key=lambda x:x['_rank'], reverse=True)[:60]
            print(f"🔹 Twitter: {len(tw_picks)} 条")

            # 3. Reddit
            rd_raw = supabase.table("raw_signals").select("*").eq("signal_type", "reddit").order("created_at", desc=True).limit(500).execute().data or []
            unique_rd_map = {}
            for r in rd_raw:
                url = r.get('url')
                if not url: continue
                curr_score = r.get('score') or 0
                if url not in unique_rd_map or curr_score > (unique_rd_map[url].get('score') or 0):
                    unique_rd_map[url] = r
            deduplicated_rd = list(unique_rd_map.values())

            def score_reddit(row):
                s = row.get('score') or 0
                v = abs(float(row.get('vibe') or 0))
                return s * (1 + v)
            sorted_rd = sorted(deduplicated_rd, key=score_reddit, reverse=True)
            
            rd_picks = []
            sub_counts = {}
            for r in sorted_rd:
                if len(rd_picks) >= 30: break
                sub = str(r.get('subreddit', 'unknown')).lower()
                if sub_counts.get(sub, 0) >= 3: continue
                rd_picks.append(r)
                sub_counts[sub] = sub_counts.get(sub, 0) + 1
            print(f"🔹 Reddit: {len(rd_picks)} 条")

            # 4. Polymarket
            poly_raw = supabase.table("raw_signals").select("*").eq("signal_type", "polymarket").order("created_at", desc=True).limit(800).execute().data or []
            unique_poly_map = {}
            for p in poly_raw:
                raw = p.get('raw_json')
                if isinstance(raw, str): 
                    try: raw = json.loads(raw)
                    except: raw = {}
                p['_parsed'] = raw
                slug = p.get('slug') or raw.get('slug')
                if not slug: continue
                curr_liq = float(p.get('liquidity') or raw.get('liquidity') or 0)
                if slug not in unique_poly_map:
                    unique_poly_map[slug] = p
                else:
                    prev_liq = float(unique_poly_map[slug].get('liquidity') or unique_poly_map[slug]['_parsed'].get('liquidity') or 0)
                    if curr_liq > prev_liq: unique_poly_map[slug] = p
            deduplicated_poly = list(unique_poly_map.values())

            def score_poly(row):
                raw = row['_parsed']
                tags = raw.get('strategy_tags', [])
                cat = str(row.get('category', '') or raw.get('category', '')).upper()
                engine = str(row.get('engine', '') or raw.get('engine', '')).lower()
                liq = float(row.get('liquidity') or raw.get('liquidity') or 0)
                base = 0
                if 'TAIL_RISK' in tags: base = 10_000_000
                elif any(x in cat for x in ['ECONOMY', 'SCIENCE', 'CLIMATE', 'TECH', 'FINANCE']): base = 5_000_000
                elif 'sniper' in engine and liq > 10000: base = 2_000_000
                elif liq > 500_000: base = 1_000_000
                return base + liq

            for r in deduplicated_poly: r['_rank'] = score_poly(r)
            poly_picks = sorted(deduplicated_poly, key=lambda x:x['_rank'], reverse=True)[:80]
            print(f"🔹 Polymarket: {len(poly_picks)} 条")

            final_batch = rare_picks + tw_picks + rd_picks + poly_picks
            print(f"🚀 全域精锐: {len(final_batch)} 条 (去重完毕)")
            return final_batch

        except Exception as e:
            print(f"⚠️ 筛选异常: {e}")
            return []

    def call_ai(self, model, sys_prompt, usr_prompt):
        if not self.api_key: return "ERROR", "No Key"
        enhanced_sys = sys_prompt + "\n[重要]：你现在是首席审计官。不要像机器人一样总结，要像索罗斯/芒格一样思考。若信号断档，请基于你的知识库推演背景。"
        payload = {
            "model": model, "messages": [{"role": "system", "content": enhanced_sys}, {"role": "user", "content": usr_prompt}],
            "temperature": 0.7, "max_tokens": 1500
        }
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        try:
            res = requests.post(self.api_url, json=payload, headers=headers, timeout=60).json()
            if 'choices' in res:
                return "SUCCESS", res['choices'][0]['message']['content']
            else:
                return "ERROR", str(res)
        except Exception as e: 
            return "ERROR", str(e)

    # =======================================================
    # 🔥 核心修正：双向同步，确保追加不报错
    # =======================================================
    def git_push_assets(self):
        if not self.vault_path: return
        cwd = self.vault_path
        
        # 1. 核心动作：先把云端最新的东西“拉”下来并合并 (Rebase)
        # 这就是解决 "Updates were rejected" 的关键
        print("🔄 [Git] 正在同步云端数据 (追加模式)...")
        subprocess.run(["git", "pull", "origin", "main", "--rebase"], cwd=cwd, check=False)
        
        # 2. 添加本地的新数据
        subprocess.run(["git", "add", "."], cwd=cwd, check=False)
        
        # 3. 提交
        if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd).returncode != 0:
            msg = f"🧠 Cognitive Audit: {datetime.now().strftime('%H:%M:%S')}"
            subprocess.run(["git", "commit", "-m", msg], cwd=cwd, check=False)
            
            # 4. 双重保险：推之前再拉一次，防止刚才这几秒云端又变了
            subprocess.run(["git", "pull", "origin", "main", "--rebase"], cwd=cwd, check=False)
            
            # 5. 放心推送
            res = subprocess.run(["git", "push", "origin", "main"], cwd=cwd, check=False)
            if res.returncode == 0:
                print("✅ [Git] 资产已追加上传！")
            else:
                print("❌ [Git] 上传失败，请检查网络。")
        else:
            print("💤 [Git] 没有新内容需要上传。")

    def audit_process(self, row, processed_ids):
        source = row.get('signal_type', 'unknown').lower()
        parts = [f"【Source: {source.upper()}】"]
        
        if source == 'github':
            parts.append(f"项目: {row.get('repo_name')} | Stars: {row.get('stars')} | Topics: {row.get('topics')}")
            parts.append(f"描述: {row.get('full_text') or '新项目发布'}")
            parts.append(f"Link: {row.get('url')}")
        elif source == 'paper':
            parts.append(f"论文: {row.get('title')} | 期刊: {row.get('journal')}")
            parts.append(f"引用: {row.get('citations')}")
            parts.append(f"摘要: {row.get('full_text')}")
        elif source in ['twitter', 'reddit']:
            parts.append(f"用户: {row.get('user_name') or row.get('subreddit')} | Score: {row.get('_rank',0)}")
            parts.append(f"内容: {row.get('full_text') or row.get('title')}")
        else: # Polymarket
            raw = row.get('_parsed') or row.get('raw_json') or {}
            if isinstance(raw, str):
                try: raw = json.loads(raw)
                except: pass
            parts.append(f"预测: {row.get('title')} | 问题: {row.get('question')}")
            prices = row.get('prices') or raw.get('outcome_prices')
            parts.append(f"价格: {prices} | 流动性: ${raw.get('liquidity')}")
            parts.append(f"标签: {raw.get('strategy_tags')} | 分类: {row.get('category')}")

        content = "\n".join(parts)
        ref_id = hashlib.sha256(content.encode()).hexdigest()
        
        if ref_id in processed_ids: return []

        results = []
        def ask_v3(s, u):
            st, r = self.call_ai(self.v3_model, s, u)
            if st == "SUCCESS" and "### Output" in r:
                return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
            if st == "SUCCESS": return "Deep Dive", r
            return None, None
        
        if not self.masters: pass

        for name, mod in self.masters.items():
            try:
                if hasattr(mod, 'audit'):
                    t, o = mod.audit(row, ask_v3)
                    if t and o:
                        results.append(json.dumps({
                            "ref_id": ref_id, "type": "V3_MASTER", "source": source,
                            "master": name, "input": content[:300].replace('\n',' '), "thought": t, "output": o
                        }, ensure_ascii=False))
                        print(f"💡 [V3-{name}] 洞察生成: {row.get('title') or row.get('full_text')[:20]}...")
            except Exception as e: 
                continue
        return results

    def process_and_ship(self, vault_path="vault"):
        self.vault_path = Path(vault_path)
        self.configure_git()
        (self.vault_path / "instructions").mkdir(parents=True, exist_ok=True)
        
        day_str = datetime.now().strftime('%Y%m%d')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}.jsonl"
        processed_ids = set()
        if output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try: processed_ids.add(json.loads(line).get('ref_id'))
                    except: pass

        signals = self.fetch_elite_signals()
        if not signals:
            print("💤 本轮无新信号入库。")
            return

        print(f"🚀 工厂全速运转: {len(signals)} 条 V3 级审计正在进行...")

        batch_size = 50
        for i in range(0, len(signals), batch_size):
            chunk = signals[i : i + batch_size]
            with ThreadPoolExecutor(max_workers=5) as executor:
                res = list(executor.map(lambda r: self.audit_process(r, processed_ids), chunk))
            
            added = []
            for r_list in res:
                if r_list: added.extend(r_list)
            
            if added:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write('\n'.join(added) + '\n')
                print(f"✨ 批次 {i//batch_size + 1} 完成 | 产出 {len(added)} 条认知资产")
                self.git_push_assets() # 50条一存

        print("🏁 任务完成。")

if __name__ == "__main__":
    factory = UniversalFactory()
    factory.process_and_ship()
