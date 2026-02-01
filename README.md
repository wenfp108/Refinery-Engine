# Refinery Engine

一个用于情报清洗与自动化的 ETL 脚本。

## 简介

本项目定期从 GitHub 私有仓库抓取原始数据（Twitter, Polymarket, GitHub），经过清洗和加权处理后，将高价值信息写入 Supabase 数据库并生成 Markdown 报告。

## 权重算法逻辑

核心逻辑位于 `processors/` 目录下，用于计算信息优先级。

### 1. Twitter 评分

逻辑文件：`processors/twitter.py`

**基础分计算：**


**修正规则：**

* **板块加权**：
* 命中硬核板块（Tech, Crypto, Finance 等）：`Score = (BaseScore + 2000) * 1.5`
* 普通板块（General）：`Score = BaseScore * 0.5`


* **噪音过滤**：
* 命中情绪化政治词（如 "woke", "scandal"）且无宏观关键词：`Score *= 0.1`


* **白名单 (VIP)**：
* 特定行业专家（如 Karpathy, Vitalik）：基础分 `+5000`



### 2. Polymarket 评分

逻辑文件：`processors/polymarket.py`

**基础公式：**


**特殊加成：**

* 命中核心关键词（Gold, BTC, Fed）：`Score *= 100`
* 标记为尾部风险（TAIL_RISK）：`Score *= 50`

### 3. GitHub 评分

逻辑文件：`processors/github.py`

* 按 `Stars` 数量降序排列。
* 同一项目只保留 Stars 最高的一条记录。

## 运行配置

**依赖环境：**

* Python 3.9+
* `pip install -r requirements.txt`

**环境变量：**
需在系统或 CI/CD Secrets 中配置：

* `GH_PAT`: GitHub Access Token (读写权限)
* `SUPABASE_URL`: 数据库地址
* `SUPABASE_KEY`: 数据库密钥

**启动命令：**

```bash
python refinery.py

```
