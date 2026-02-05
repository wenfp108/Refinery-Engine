import pandas as pd
import hashlib
import json
import os
import importlib.util
import sys
from pathlib import Path
from datetime import datetime  # ✅ 新增：用于生成月份文件名

class UniversalFactory:
    def __init__(self, masters_path="masters"):
        # 使用 Path 对象，处理路径更安全
        self.masters_path = Path(masters_path)
        self.masters = self._load_masters()

    def _load_masters(self):
        masters = {}
        if not self.masters_path.exists():
            print(f"⚠️ [警告] 大师目录不存在: {self.masters_path}")
            return masters

        # 遍历 .py 文件
        for file_path in self.masters_path.glob("*.py"):
            if file_path.name.startswith("__"): continue
            
            try:
                name = file_path.stem # 获取文件名（无后缀）
                spec = importlib.util.spec_from_file_location(name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # 检查是否有 audit 函数，确保是有效插件
                if hasattr(module, 'audit'):
                    masters[name] = module
                    print(f"✅ [加载成功] 认知插件: {name}")
                else:
                    print(f"⚠️ [跳过] {name} 缺少 audit() 函数")
            except Exception as e:
                print(f"❌ [加载失败] {file_path.name}: {e}")
        
        return masters

    def generate_ref_id(self, row_dict):
        """生成永久哈希 ID (增强通用性)"""
        # 优先使用 Polymarket 特征
        p1 = str(row_dict.get('eventTitle') or '')
        p2 = str(row_dict.get('question') or '')
        content = f"{p1}{p2}"
        
        # 🛡️ 通用兜底：如果不是 Polymarket 数据，则使用整行数据的哈希
        if not p1 and not p2:
            # sort_keys确保字典顺序一致，保证哈希唯一性
            content = json.dumps(row_dict, sort_keys=True, default=str)
            
        return hashlib.sha256(content.encode()).hexdigest()

    def process_and_ship(self, input_raw, vault_path, batch_size=2000):
        """加工并送回中央银行 (流式写入 + 按月分卷版)"""
        input_path = Path(input_raw)
        vault_dir = Path(vault_path)
        
        if not input_path.exists():
            print(f"❌ [错误] 找不到原始归档: {input_path}")
            return

        # 1. 读取数据
        try:
            df = pd.read_parquet(input_path)
            print(f"🏭 工厂启动: 正在处理 {len(df)} 条原始信号，调用 {len(self.masters)} 位大师...")
        except Exception as e:
            print(f"❌ Parquet 读取失败: {e}")
            return

        # 🔥 [关键修改] 动态生成带月份的文件名，防止单文件过大
        # 结果示例: instructions/teachings_202602.jsonl
        current_month = datetime.now().strftime('%Y%m') 
        filename = f"teachings_{current_month}.jsonl"
        
        # 准备输出文件 (自动创建父文件夹)
        output_file = vault_dir / "instructions" / filename
        output_file.parent.mkdir(parents=True, exist_ok=True)

        buffer = []
        count = 0
        
        # 🚀 性能优化: to_dict('records') 比 iterrows 快几十倍
        rows = df.to_dict('records')

        # 使用 append 模式打开，支持断点续写
        with open(output_file, 'a', encoding='utf-8') as f:
            for row_dict in rows:
                ref_id = self.generate_ref_id(row_dict)
                # 兼容不同数据源的标题字段 (Polymarket用eventTitle, Twitter用full_text前20字)
                event_title = row_dict.get('eventTitle') or row_dict.get('full_text', '')[:20] or '未命名信号'

                # 并行审计 (逻辑层面)
                for master_name, master_mod in self.masters.items():
                    try:
                        # 获取版本号，默认为 1.0
                        ver = getattr(master_mod, "VERSION", "1.0")
                        
                        # 🛡️ 熔断保护：防止单个大师报错卡死整个流程
                        thought, output = master_mod.audit(row_dict)

                        # 只有大师有话要说(返回非空)时才记录
                        if thought or output:
                            entry = {
                                "ref_id": ref_id,
                                "master": master_name,
                                "version": ver,
                                "instruction": f"请分析事件: {event_title}",
                                "thought": thought,
                                "output": output
                            }
                            buffer.append(json.dumps(entry, ensure_ascii=False))
                        
                    except Exception as e:
                        # 仅打印错误，不中断循环
                        # print(f"⚠️ [{master_name}] 审计失败: {e}") 
                        pass

                # 🚀 内存保护: 积攒到 batch_size 再写入硬盘
                if len(buffer) >= batch_size:
                    f.write('\n'.join(buffer) + '\n')
                    count += len(buffer)
                    buffer = [] # 清空缓冲区
            
            # 写入剩余数据
            if buffer:
                f.write('\n'.join(buffer) + '\n')
                count += len(buffer)

        print(f"🚀 [发货完成] 已将 {count} 条认知资产注入中央银行: {output_file}")

if __name__ == "__main__":
    # 示例调用
    factory = UniversalFactory(masters_path="../Masters-Council/masters")
    factory.process_and_ship(
        input_raw="temp_raw.parquet", 
        vault_path="../Central-Bank"
    )
