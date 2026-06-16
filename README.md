# Ray Data Demo

Apache Ray Data 功能演示项目，专为架构师客户演示设计。

## 项目结构

```
ray_demo/
├── app.py                 # Flask 后端服务（80+ 路由：基础 demo / 场景 / 故事 / ROI / 卡片）
├── start.sh               # 一键启动脚本
├── requirements.txt
├── backend/
│   ├── config.py          # 配置中心
│   ├── core/task_manager.py
│   ├── engines/           # Ray / Spark 引擎抽象（base + ray + spark）
│   ├── scenarios/         # Ray Data 15 场景实验室（catalog + dataops + training + serve…）
│   ├── stories/           # 【M1 销售路演】行业故事
│   │   ├── llm_dedup.py       # 故事 A：大模型数据清洗（dedup/score/tokenize 全 ray.data 真跑）
│   │   ├── video_tagging.py   # 故事 B：多模态打标（CLIPActor 全局 pool + numpy 模拟推理）
│   │   └── __init__.py        # list_stories / get_story_runner / warmup_*
│   └── pricing/           # 【M1 销售路演】ROI 计算器
│       └── tencent_cloud.py   # 腾讯云 ap-guangzhou/sh/svl 价格表 + saved_usd/percent/months 锚点
├── static/
│   ├── index.html         # 主页：4 实测 Demo + 架构动画 + 顶部 nav
│   ├── scenarios.html     # 15 场景实验室
│   ├── stories.html       # 【M1 D2】🎬 行业故事舞台（双故事 + DAG + ROI 双柱）
│   └── cards.html         # 【M1 D4】💎 销售金句卡（5 张轮播 + 录 webm）
├── docs/
│   └── M1_design.md       # M1 销售路演 5 天冲刺设计文档
└── README.md
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动服务

```bash
python app.py
```

服务将在 `http://localhost:5000` 启动

### 3. 打开浏览器

访问 `http://localhost:5000`

## 演示内容

### Ray vs Spark 对比（5 个核心区别）

| 维度 | Ray Data | Spark SQL |
|------|----------|-----------|
| **1. 计算资源** | GPU + CPU 异构计算 | 主要 CPU，GPU 支持有限 |
| **2. 数据类型** | 非结构化（视频/图像/文本）+ 结构化 | 结构化 + 半结构化（SQL 优化） |
| **3. 主要用途** | AI 训练、批量推理、模型服务 | ETL、BI 分析、经典 ML |
| **4. 并行模型** | Actor 模型 + 细粒度任务并行 | 数据并行（RDD/DataFrame） |
| **5. 扩展 Python** | 原生支持，添加几行代码即可扩展 | 需要适配数据并行模型 |

### Demo 1: 数据预处理流水线
- 展示 Ray Data 的 Map、Filter、FlatMap 分布式操作
- 价值点：分布式处理、链式操作、自动并行

### Demo 2: 批量推理 (Batch Inference)
- 使用 Ray Data 进行大规模 LLM 推理
- 价值点：批量处理、高吞吐量、情感分析示例

### Demo 3: 流式数据加载
- 展示 GPU 预取和流式数据处理
- 价值点：流式读取、GPU 预取、最大化 GPU 利用率

### Demo 4: Ray vs Spark 实测对比
- 同一条流水线（创建 → Map → Filter → 聚合）分别在 Ray Data 和本地 Spark 3.5.8 上各跑一遍
- 价值点：结果一致性校验、真实耗时对比、引擎抽象（统一 `pipeline()` 接口）
- 依赖本地 Spark 环境，路径在 `backend/config.py` 的 `SPARK_HOME` 配置

### Demo 5: Ray 架构动画 — 细粒度并行 / Actor 模型 / 数据流
用 3 段纯前端 SVG 动画把 Ray 区别于 Spark 的核心机制讲清楚（架构师视角）：

- **场景 1 · 细粒度并行**
  - Ray：Driver 不停往 8 个 Worker 喷射成千上万个轻量 task（毫秒级、可异构调度）
  - Spark：partition **就地计算**（数据本地性优先，并不"整块迁移"），4 条 task 进度条跑完才在 **Stage Barrier** 处闪烁 shuffle，速度由最慢的 task 决定 → 长尾问题
- **场景 2 · Actor 模型 vs Spark 无 Actor**
  - Ray：Caller 反复调用 LLMWorker Actor，模型/GPU/连接 **常驻** 内存
  - Spark：Driver `broadcast(model)` → 每个 Executor 收到 task closure 都要 **反序列化、重新 load model、重开 DB 连接、无 GPU 缓存**（每次冷启动）
- **场景 3 · 数据流 DAG vs Stage Barrier**
  - Ray：block 级流水线，多个 block 并行穿过 Source → Map → Filter → GPU → Sink，下游 GPU 不必等上游完成
  - Spark：4 个 task 速度不一致（数据倾斜），快 task 提前到 barrier 处"**插旗等待**"，barrier 标签实时显示 `⏸ 我在等… 2/4`，4 个状态圆点指示哪些 task 已到达，**全员到齐才放行进入下一 Stage**。慢 task 决定整体节奏，GPU/下游全部空转 → 直观展示"等长尾"痛点。

> 入口：主界面顶部 Tab 切到 "架构动画"；动画自动循环，可点击 ① / ② / ③ 切换场景。

## Ray Data vs Spark SQL 详细对比

### 1. 计算资源
- **Ray Data**: 原生支持 CPU + GPU 异构计算，最大化 GPU 利用率
- **Spark SQL**: 主要面向 CPU 环境，GPU 支持需要额外配置

### 2. 数据类型
- **Ray Data**: 擅长处理非结构化数据（视频、图像、文本等多模态数据）
- **Spark SQL**: SQL 和 DataFrame API 优化用于结构化和半结构化数据

### 3. 主要用途
- **Ray Data**: 面向 AI 工作负载，包括 LLM 训练、批量推理、模型服务
- **Spark SQL**: 面向传统 BI 分析、ETL、经典机器学习

### 4. 并行模型
- **Ray Data**: Actor 模型，提供细粒度任务并行，灵活性更高
- **Spark SQL**: 数据并行计算模型（RDD/DataFrame），适合数据工作负载

### 5. 扩展 Python
- **Ray Data**: 原生支持任意 Python 程序扩展，添加几行代码即可
- **Spark SQL**: 需要适配数据并行模型，灵活性有限

## Amazon 迁移案例

从 Spark 迁移到 Ray 后：
- 数据集规模提升 **12 倍**
- 成本效率提升 **91%**
- 每小时数据处理量提升 **13 倍**
- 年节省成本 **1.2 亿美元**

## 前端功能

- Ray vs Spark 对比卡片（5 个核心区别）
- 详细对比表格
- Amazon 案例数据展示
- Tab 切换 3 个 Demo
- 实时进度条显示
- 日志输出面板
- 统计指标卡片
- Ray Data 代码示例

## API 接口

### 基础 / 演示
| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/health` | GET | 健康检查 |
| `/api/ray/init` | POST | 初始化 Ray |
| `/api/engines` | GET | 列出已注册引擎（ray / spark） |
| `/api/demo{1,2,3}/start` | POST | 启动 3 个基础 Demo |
| `/api/compare/start` | POST | Ray vs Spark 实测对比（body: `num_rows`, `threshold`） |
| `/api/scenarios` | GET | 15 场景实验室元数据 |
| `/api/scenarios/<id>/start` | POST | 启动指定场景 |
| `/api/tasks/<task_id>` | GET | 获取任务状态 |

### 销售路演（M1 新增）
| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/stories` | GET | 行业故事元数据列表（含 speedup / 痛点叙事） |
| `/api/stories/warmup` | POST | 一次性预热 Ray Data + video CLIPActor pool |
| `/api/story/<story_id>/start` | POST | 登记故事任务，返回 `task_id`（body: 见下） |
| `/api/story/<story_id>/<task_id>/stream` | GET | **SSE 流**：`progress` / `metric` / `done` / `close` 事件 |
| `/api/pricing/calculate` | POST | ROI 账单（body: `scenario`, `ray_ms`, `spark_ms`, `gpu_count`, `region`） |
| `/api/pricing/regions` | GET | 支持的腾讯云地域列表 |
| `/api/cards/manifest` | GET | 5 张销售金句卡的元数据 |

故事任务 body 示例：
```json
// llm_dedup
{ "token_size": 1000000000, "gpu_count": 8, "region": "ap-guangzhou", "speedup_target": 5.6 }
// video_tagging
{ "video_count": 1000, "gpu_count": 8, "region": "ap-guangzhou", "speedup_target": 14.0 }
```

### 前端入口

| 路径 | 用途 |
|------|------|
| `/` | 主页（4 实测 Demo + 架构动画 + 顶部 nav 跳转） |
| `/scenarios` | Ray Data 15 场景实验室 |
| `/stories` | **🎬 行业故事舞台**（故事 A 大模型清洗 / 故事 B 多模态打标 + ROI 账单） |
| `/cards` | **💎 销售金句卡**（5 张轮播 + 一键录 webm） |

## 技术栈

- **后端**: Flask + Ray Data 2.55.1
- **前端**: HTML5 + CSS3 + 原生 JavaScript（无构建工具）
- **数据处理**: Ray Data（ray.data.range / Actor pool / GPU stage）
- **录制**: MediaRecorder + getDisplayMedia（webm 直接下载）

---

## 销售路演动线脚本（M1 D5 校准）

### 🎯 动线 A：90 秒电梯 Pitch（销售自服务）

**场景**：客户 BD 见面、cold call、展会扫码后转化。

```
1. 打开 /cards            ⏱ 0-50s   5 张金句卡自动 10s 轮播
   口播：「同一份 AI 数据负载，Spark 18% GPU 利用 vs Ray 89%；
          Spark 每 task 重 load 模型 12s vs Ray Actor 0ms 常驻；
          长尾 task 拖死 stage barrier vs Ray 流式 DAG；
          AI 数据是流不是表；100 万视频打标 $8.4k → $1.2k」
2. 点 ⏺ 录当前卡 10s     ⏱ 50-60s  现场录 webm，发给客户群

[转化目标：留二次约谈机会]
```

### 🎯 动线 B：5 分钟客户故事（CTO/架构师对话）

**场景**：客户技术决策人在场，要拿数字说话。

```
1. /stories  →  故事 A 大模型清洗      ⏱ 0-90s
   - 念痛点 quote（Top3 大模型公司 / 36h / $23k）
   - 点 ▶ 开跑，规模 1B token，看 5 阶段 DAG 节点逐个亮起
   - 实时指标条：Ray GPU 89% vs Spark GPU 18%
   - 完成后右侧 ROI 账单：省 $908 / 单次 · 92% 折扣
   - 锚点：等价于免费用 HAI A100 X 个月，全年 $360k

2. /stories  →  故事 B 多模态打标      ⏱ 90-180s
   - 念痛点（5 亿视频/天 · CLIP 12s 冷启 · 200 张 A100 跑不完）
   - 点 ▶ 开跑，规模 1000 视频
   - 关键看点：「这是第 N 次跑，actor uptime 已经累计 600+ 秒」
     → 戳出 \"Actor 全局复用，模型 load 全局只付 1 次\" 的核心论点
   - 实测 11.9× 加速 · 集群 1900 fps · 单卡推算 12 fps
   - ROI：单次省 $206 · 91% · 全年放大 $75k

3. /                       ⏱ 180-240s
   架构动画 Demo 5 → 切到 ③ 数据流 DAG
   - 现场对比：Ray 完成 30 个 block 的同时 Spark 才完成 1 轮
   - 状态行直接念：「同期 Spark 才完成 1 轮，流水线密度碾压」

4. 收尾留 30s 答疑 + 留资料  ⏱ 240-300s

[转化目标：进 POC 阶段，资源券扫码申请]
```

### 🎯 动线 C：30 分钟大客户深聊（POC 立项前夜）

```
1.  /            10min   主页基础概念 + 4 实测 Demo + 架构动画 3 段
2.  /scenarios   10min   15 场景实验室，针对客户业务点 3-4 个细谈
3.  /stories     5min    故事 A + B 完整跑，ROI 数字按客户实际规模重算
                         （改 GPU 数 / 地域 / 业务规模 -> 一键重新计算）
4.  /cards       5min    5 张卡录屏带回，作为内部汇报材料
```

---

## 演示建议（基础动线）

1. 先展示 Ray vs Spark 对比，说明核心区别
2. 介绍 Amazon 案例数据，展示实际价值
3. 依次演示 3 个场景，每个场景 30-60 秒
4. 强调 Ray Data 相比 Spark 的优势
5. 展示代码示例，说明 API 简洁易用

## 故障排查

- **SSE 连接立刻断**：检查 Flask 是否启用 `stream_with_context` + `X-Accel-Buffering: no`（已配）
- **首次点击故事按钮卡 5-8s**：未触发 warmup，刷新页面会自动 POST `/api/stories/warmup`
- **video_tagging actor processed 不累计**：`STORY_FORCE_MOCK=1` 环境变量没清；重启 `app.py` 即可
- **录 webm 弹屏幕共享窗口**：浏览器策略，必须用户手动选择窗口；选当前 Tab 即可

