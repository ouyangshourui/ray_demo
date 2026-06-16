# Ray Data Demo

Apache Ray Data 功能演示项目，专为架构师客户演示设计。

## 项目结构

```
ray_demo/
├── app.py                 # Flask 后端服务（含 Ray vs Spark 实测对比）
├── requirements.txt       # Python 依赖
├── backend/               # 引擎抽象 + 任务管理
│   ├── config.py          # 配置中心（Spark/服务端口等）
│   ├── core/
│   │   └── task_manager.py    # 线程安全的异步任务管理器
│   └── engines/
│       ├── base.py            # 引擎抽象基类（统一 pipeline 接口）
│       ├── ray_engine.py      # Ray Data 引擎实现
│       ├── spark_engine.py    # 本地 Spark 3.5.8 引擎实现
│       └── __init__.py        # 引擎注册中心 get_engine / list_engines
├── static/
│   └── index.html         # 前端演示页面（4 个 Demo）
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

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/health` | GET | 健康检查 |
| `/api/ray/init` | POST | 初始化 Ray |
| `/api/engines` | GET | 列出已注册引擎（ray / spark） |
| `/api/demo1/start` | POST | 启动 Demo 1 |
| `/api/demo2/start` | POST | 启动 Demo 2 |
| `/api/demo3/start` | POST | 启动 Demo 3 |
| `/api/compare/start` | POST | 启动 Ray vs Spark 实测对比（body: `num_rows`, `threshold`） |
| `/api/tasks/<task_id>` | GET | 获取任务状态 |

## 技术栈

- **后端**: Flask + Ray Data 2.55.1
- **前端**: HTML5 + CSS3 + JavaScript
- **数据处理**: Ray Data

## 演示建议

1. 先展示 Ray vs Spark 对比，说明核心区别
2. 介绍 Amazon 案例数据，展示实际价值
3. 依次演示 3 个场景，每个场景 30-60 秒
4. 强调 Ray Data 相比 Spark 的优势
5. 展示代码示例，说明 API 简洁易用
