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
        self.api_key = os.environ.get("SILICON_FLOW_KEY") # 确保环境变量里有这个
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.vault_path = None
        
        # 🛡️ 校验配置
        if not all([self.api_key, self.supabase_url, self.supabase_key]):
            print("❌ [Factory] 启动失败: 环境变量缺失 (SILICON_FLOW_KEY, SUPABASE_URL, SUPABASE_KEY)")
            sys.exit(1)
        
        # 🤖 模型设定：全员 V3，废弃 Scout
        self.v3_model = "deepseek-ai/DeepSeek-V3"

    def _load_masters(self):
        masters = {}
        if not self.masters_path.exists(): 
            # 如果目录不存在，自动创建（防止报错）
            try:
                self.masters_path.mkdir(exist_ok=True)
            except:
                pass
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
        # 确保 Vault 目录存在
        if not self.vault_path.exists():
            self.vault_path.mkdir(parents=True, exist_ok=True)
            
        subprocess.run(["git", "config", "--global", "user.email", "bot@factory.com"], check=False)
        subprocess.run(["git", "config", "--global", "user.name", "Cognitive Bot"], check=False)

    def fetch_elite_signals(self):
        """
        🌟 核心逻辑：精锐席位筛选
        特性：
        1. 去重盾 (Dedup Shield): Polymarket 按 Slug 去重
        2. 狙击手保护 (Sniper Protection): Sniper 信号独立加权
        3. 标签雷达 (Smart Radar): 强制插队经济/科学/科技
        4. 板块熔断 (Subreddit Cap): Reddit 每个板块限 3 条
        """
        try:
            supabase = create_client(self.supabase_url, self.supabase_key)
            print("💎 启动精锐筛选 (目标: ~220 条 | 启用严格去重)...")

            # ==========================================
            # 1. GitHub & Paper: 全量 (上限 50)
            # ==========================================
            rare_raw = supabase.table("raw_signals") \
                .select("*") \
                .or_("signal_type.eq.github,signal_type.eq.paper") \
                .order("created_at", desc=True) \
                .limit(50).execute().data or []
            
            # 简单去重 (保留最新)
            unique_rare = {}
            for r in rare_raw:
                k = r.get('repo_name') or r.get('title')
                if k and k not in unique_rare: unique_rare[k] = r
            rare_picks = list(unique_rare.values())
            print(f"🔹 稀缺源: {len(rare_picks)} 条")

            # ==========================================
            # 2. Twitter: Top 60 (VIP + Viral)
            # ==========================================
            tw_raw = supabase.table("raw_signals").select("*").eq("signal_type", "twitter").order("created_at", desc=True).limit(500).execute().data or []
            vip_list = ['Karpathy', 'Musk', 'Vitalik', 'LeCun', 'Dalio', 'Naval', 'Sama', 'PaulG']
            
            def score_twitter(row):
                rt = row.get('retweets') or 0
                bm = row.get('bookmarks') or 0
                like = row.get('likes') or 0
                user = str(row.get('user_name', '')).lower()
                
                # 基础分：(RT x 5) + (BM x 10) + Like
                # 🔧 修正：应对数据中 Bookmark 为 0 的情况，如果 RT 极高，给予额外补偿
                score = (rt * 5) + (bm * 10) + like
                if rt > 10000: score += 5000  # 病毒式传播补偿
                
                # VIP 加权
                is_vip = any(v.lower() in user for v in vip_list)
                if is_vip:
                    # 只有当 VIP 的推文稍微有点热度时才加分，防止垃圾刷屏
                    if rt > 10 or like > 50: score += 10000
                    else: score += 500 # 纯水贴只加一点点
                
                return score

            for r in tw_raw: r['_rank'] = score_twitter(r)
            tw_picks = sorted(tw_raw, key=lambda x:x['_rank'], reverse=True)[:60]
            print(f"🔹 Twitter: {len(tw_picks)} 条")

            # ==========================================
            # 3. Reddit: Top 30 (去重 + 板块熔断)
            # ==========================================
            rd_raw = supabase.table("raw_signals").select("*").eq("signal_type", "reddit").order("created_at", desc=True).limit(500).execute().data or []

            # A. URL 去重
            unique_rd_map = {}
            for r in rd_raw:
                url = r.get('url')
                if not url: continue
                curr_score = r.get('score') or 0
                # 如果 URL 已存在，保留分数更高的那个
                if url not in unique_rd_map or curr_score > (unique_rd_map[url].get('score') or 0):
                    unique_rd_map[url] = r
            deduplicated_rd = list(unique_rd_map.values())

            # B. 打分
            def score_reddit(row):
                s = row.get('score') or 0
                v = abs(float(row.get('vibe') or 0))
                return s * (1 + v)

            sorted_rd = sorted(deduplicated_rd, key=score_reddit, reverse=True)
            
            # C. 板块熔断 (每个 Subreddit 限 3 条)
            rd_picks = []
            sub_counts = {}
            for r in sorted_rd:
                if len(rd_picks) >= 30: break
                sub = str(r.get('subreddit', 'unknown')).lower()
                if sub_counts.get(sub, 0) >= 3: continue
                rd_picks.append(r)
                sub_counts[sub] = sub_counts.get(sub, 0) + 1
            
            print(f"🔹 Reddit: {len(rd_picks)} 条 (Top 30 | 已熔断)")

            # ==========================================
            # 4. Polymarket: Top 80 (去重 + 智能分层)
            # ⚠️ 您之前强调要 80 条，我这里帮您改回 80 (原代码是 60)
            # ==========================================
            poly_raw = supabase.table("raw_signals").select("*").eq("signal_type", "polymarket").order("created_at", desc=True).limit(800).execute().data or []

            # A. Slug 去重
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
                
                # 保留流动性更好的那个版本
                if slug not in unique_poly_map:
                    unique_poly_map[slug] = p
                else:
                    prev_liq = float(unique_poly_map[slug].get('liquidity') or unique_poly_map[slug]['_parsed'].get('liquidity') or 0)
                    if curr_liq > prev_liq: unique_poly_map[slug] = p
            
            deduplicated_poly = list(unique_poly_map.values())

            # B. 智能打分 (四级准入)
            def score_poly(row):
                raw = row['_parsed']
                tags = raw.get('strategy_tags', [])
                cat = str(row.get('category', '') or raw.get('category', '')).upper()
                engine = str(row.get('engine', '') or raw.get('engine', '')).lower()
                liq = float(row.get('liquidity') or raw.get('liquidity') or 0)

                base = 0
                # 👑 Tier 1: 黑天鹅
                if 'TAIL_RISK' in tags: base = 10_000_000
                # 🚀 Tier 2: 核心叙事 (ECONOMY/SCIENCE/TECH)
                elif any(x in cat for x in ['ECONOMY', 'SCIENCE', 'CLIMATE', 'TECH', 'FINANCE']): base = 5_000_000
                # 🔫 Tier 3: Sniper 保护
                elif 'sniper' in engine and liq > 10000: base = 2_000_000
                # 💰 Tier 4: 大资金
                elif liq > 500_000: base = 1_000_000
                
                return base + liq

            for r in deduplicated_poly: r['_rank'] = score_poly(r)
            # 🔥 修正：取前 80 条
            poly_picks = sorted(deduplicated_poly, key=lambda x:x['_rank'], reverse=True)[:80]
            print(f"🔹 Polymarket: {len(poly_picks)} 条 (Top 80)")

            # ==========================================
            # 5. 最终集结 (宁缺毋滥，不补位)
            # ==========================================
            final_batch = rare_picks + tw_picks + rd_picks + poly_picks
            print(f"🚀 全域精锐: {len(final_batch)} 条 (去重完毕)")
            return final_batch

        except Exception as e:
            print(f"⚠️ 筛选异常: {e} (启动安全模式)")
            import traceback
            traceback.print_exc()
            return []

    def call_ai(self, model, sys_prompt, usr_prompt):
        if not self.api_key: return "ERROR", "No Key"
        # 🧠 注入‘逻辑接骨’指令
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
                print(f"❌ AI API Error: {res}")
                return "ERROR", str(res)
        except Exception as e: 
            return "ERROR", str(e)

    def git_push_assets(self):
        if not self.vault_path: return
        cwd = self.vault_path
        # 简单检查 git 是否可用
        if not (cwd / ".git").exists():
             subprocess.run(["git", "init"], cwd=cwd, check=False)
             subprocess.run(["git", "remote", "add", "origin", f"https://oauth2:{os.environ.get('GH_PAT')}@github.com/wenfp108/vault.git"], cwd=cwd, check=False)

        subprocess.run(["git", "add", "."], cwd=cwd, check=False)
        if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd).returncode != 0:
            subprocess.run(["git", "commit", "-m", f"🧠 Cognitive Audit: {datetime.now().strftime('%H:%M:%S')}"], cwd=cwd, check=False)
            subprocess.run(["git", "push", "origin", "main"], cwd=cwd, check=False)

    def audit_process(self, row, processed_ids):
        # === 1. 构建上下文 ===
        source = row.get('signal_type', 'unknown').lower()
        parts = [f"【Source: {source.upper()}】"]
        
        # 增强上下文构建
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
        # === 2. 强制 V3 审计 (No Scout) ===
        def ask_v3(s, u):
            st, r = self.call_ai(self.v3_model, s, u)
            if st == "SUCCESS" and "### Output" in r:
                return r.split("### Output")[0].replace("### Thought","").strip(), r.split("### Output")[1].strip()
            if st == "SUCCESS": return "Deep Dive", r
            return None, None
        
        # 如果没有 Masters 插件，默认跳过 (或者可以加一个默认审计逻辑)
        if not self.masters:
            # print("⚠️ No masters loaded.") 
            pass

        for name, mod in self.masters.items():
            try:
                if hasattr(mod, 'audit'):
                    # 只有当 Master 认为值得审计时才返回 (Thought, Output)
                    t, o = mod.audit(row, ask_v3)
                    if t and o:
                        results.append(json.dumps({
                            "ref_id": ref_id, "type": "V3_MASTER", "source": source,
                            "master": name, "input": content[:300].replace('\n',' '), "thought": t, "output": o
                        }, ensure_ascii=False))
                        print(f"💡 [V3-{name}] 洞察生成: {row.get('title') or row.get('full_text')[:20]}...")
            except Exception as e: 
                # print(f"⚠️ Master {name} audit error: {e}")
                continue
        return results

    def process_and_ship(self, vault_path="vault"):
        self.vault_path = Path(vault_path)
        self.configure_git()
        
        # 确保目录结构
        (self.vault_path / "instructions").mkdir(parents=True, exist_ok=True)
        
        # 加载去重 ID
        day_str = datetime.now().strftime('%Y%m%d')
        output_file = self.vault_path / "instructions" / f"teachings_{day_str}.jsonl"
        processed_ids = set()
        if output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try: processed_ids.add(json.loads(line).get('ref_id'))
                    except: pass

        # 🌟 获取精锐信号
        signals = self.fetch_elite_signals()
        if not signals:
            print("💤 本轮无新信号入库。")
            return

        print(f"🚀 工厂全速运转: {len(signals)} 条 V3 级审计正在进行...")

        batch_size = 50
        # 限制并发数为 5，避免 API 限流
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
    # 实例化并运行
    factory = UniversalFactory()
    factory.process_and_ship()
