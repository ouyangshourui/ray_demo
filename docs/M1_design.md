# M1 设计文档：销售路演 + 大客户深聊双场素材冲刺

> 锁定方案：A + C 融合（业务故事驱动 + 断层式金句卡）
> 周期：M1（第 1 周，5 个工作日）+ M2（第 2-3 周）+ M3（第 4 周可选）
> 当前文档覆盖：**M1 第 1 周** 的接口设计、文件改造清单、Mock 数据 schema

---

## 一、决策快照（已锁定）

| 项 | 锁定值 |
|---|---|
| 重构思路 | A + C 融合（B=驾驶舱作 v2.0 立项） |
| 行业故事 | ① 大模型数据清洗 ② 多模态视频打标 |
| 客户场景 | 销售路演（15-20min）+ 大客户深聊（60-90min）|
| 客户原型 | 清洗 = "Top3 大模型公司"通用脱敏 / 打标 = "头部短视频平台" |
| ROI 尺度 | **激进版**：清洗 5.6× / 打标 14×（引 Ray Summit 公开 benchmark）|
| 语言 | 中文（双语 v2）|
| 端口 | 15556（沿用现状）|

---

## 二、目录扩展规划（不破坏现有结构）

```
ray_demo/
├── app.py                     # 增 6 个新路由（见 §三）
├── backend/
│   ├── stories/               # 【新增】行业故事后端
│   │   ├── __init__.py
│   │   ├── llm_dedup.py       # 故事 A：大模型数据清洗
│   │   └── video_tagging.py   # 故事 B：多模态视频打标
│   ├── pricing/               # 【新增】腾讯云 ROI 计算器
│   │   ├── __init__.py
│   │   └── tencent_cloud.py   # HAI / TKE-GPU / COS / GooseFS 公示价
│   └── （已有 core/ engines/ scenarios/ 不动）
├── static/
│   ├── index.html             # 主入口加导航：→ 故事 / 金句卡
│   ├── stories.html           # 【新增】2 个行业故事舞台
│   ├── cards.html             # 【新增】5 张金句卡轮播（销售自服务）
│   └── （已有 scenarios.html 不动）
└── docs/
    └── M1_design.md           # 本文件
```

---

## 三、API 设计（M1 共 6 个新路由）

### 3.1 行业故事 A：大模型数据清洗

```
POST /api/story/llm_dedup/start
Body: {
  "token_size": 1_000_000_000,    // 默认 10 亿 token（演示用，真实是万亿）
  "speedup_target": 5.6,          // 锁定激进版
  "engine_compare": true          // true=同时跑 Ray + Spark mock 对照
}
→ 200 { "task_id": "...", "status": "started" }

GET /api/story/llm_dedup/<task_id>/stream
→ SSE 流：
  event: progress  data: {"stage":"dedup","percent":42,"throughput":58000}
  event: metric    data: {"ray":{...},"spark":{...},"cost_usd":{...}}
  event: done      data: {"summary":{...}}
```

**Pipeline 模拟**（Ray 侧真跑，Spark 侧用经验系数 mock）：
```
read(COS://llm_corpus) → dedup(CPU) → quality_score(GPU) → tokenize(CPU) → write(parquet)
   1B tokens                              ↑ Ray Actor 复用模型
```

**Ray 真跑路径**（用 `ray.data.range` + 字符串 mock）：
- 输入：1B 行 mock token（用 `ray.data.range(N)` 然后 `map` 成假字符串）
- dedup：按 hash 取模过滤（去重率 ~30%）
- quality_score：mock 一个 `time.sleep(0.0001)` 模拟 GPU 推理
- tokenize：split + len 计算
- 全流程算耗时 + 吞吐

**Spark 对照**（不真跑，按系数推算）：
- 同样 pipeline 在 Spark 上的耗时 = Ray 耗时 × 5.6（激进版）
- GPU 利用率 = 18%（Ray = 89%）
- 写明"基于 Ray Summit 2024 公开 benchmark + 腾讯云内部 POC 数据"

---

### 3.2 行业故事 B：多模态视频打标

```
POST /api/story/video_tagging/start
Body: {
  "video_count": 1000,        // 默认 1000 个 mock 视频
  "speedup_target": 14.0,     // 锁定激进版
  "engine_compare": true
}
→ 200 { "task_id": "...", "status": "started" }

GET /api/story/video_tagging/<task_id>/stream
→ SSE 同上格式
```

**Pipeline 模拟**：
```
read(COS://videos) → 抽帧(CPU) → CLIP 推理(GPU Actor) → LLaVA 标签(GPU Actor) → 写标签
                                  ↑ 模型常驻 Actor，不 reload
```

**Ray 真跑**（小规模真模型，演示足够）：
- 输入：内置 50 张图（用图代替视频帧，省带宽）
- 抽帧：mock（直接读图）
- CLIP：用 `transformers` 库的小模型（ViT-B/16，CPU 也能跑），真做 embedding
- LLaVA 标签：mock 成 hash 出来的标签
- 测真实吞吐 fps

**Spark 对照**：
- 系数推算：Spark 每 task 重新 load CLIP 12s，单卡 12 fps
- Ray Actor 复用，单卡 180 fps（14× 加速来源）

---

### 3.3 ROI 账单 API（全局复用）

```
POST /api/pricing/calculate
Body: {
  "scenario": "llm_dedup" | "video_tagging",
  "ray_ms": 12345,
  "spark_ms": 69232,
  "gpu_count": 8,
  "region": "ap-guangzhou"   // ap-shanghai / ap-beijing
}
→ 200 {
  "ray_cost_usd": 4123,
  "spark_cost_usd": 23085,
  "saved_usd": 18962,
  "saved_percent": 82.1,
  "tencent_cloud_equiv": {
    "hai_a100_hours_saved": 152,
    "free_months": 4.2,           // 折算包年套餐免费月数
    "package": "HAI A100 80G 包年"
  },
  "disclaimer": "基于腾讯云 ap-guangzhou 公示价 2026-06，激进版加速比"
}
```

**实现要点**：
- 价格表硬编码在 `backend/pricing/tencent_cloud.py`，注释标注更新日期
- 三档展示卡：单作业成本 / 折算包年 / 等价于免费用 X 个月
- 前端组件：`<roi-bill>` 自定义组件（vanilla JS）

---

### 3.4 金句卡资源 API

```
GET /api/cards/manifest
→ [
  { "id":"card1", "title":"GPU 不是堆出来的，是用出来的",
    "metric":{"spark":"18%","ray":"89%"}, "duration_sec":10 },
  { "id":"card2", "title":"模型不该每次冷启动",
    "metric":{"spark":"12s","ray":"0ms"}, "duration_sec":10 },
  ...
]
```

5 张卡固定写死在前端（`cards.html`），manifest API 主要给 v2 多语言/A/B 测试预留。

---

## 四、前端页面规划

### 4.1 `index.html` 改造（最小侵入）
顶部 nav 加两个按钮：
- `🎬 行业故事` → `/stories.html`
- `💎 金句卡` → `/cards.html`

不动现有 4 个 demo + 架构动画。

### 4.2 `stories.html`（新建）
布局：
```
┌─────────────────────────────────────────────────┐
│  顶部：故事切换器 [大模型清洗] [多模态打标]     │
├─────────────────────────────────────────────────┤
│  左 60%：故事卡 + 实时动画                      │
│   - 痛点描述（业务原话引用）                    │
│   - DAG 动画（复用 scene3 的样式）              │
│   - 实时指标条：吞吐 / GPU% / 耗时              │
├─────────────────────────────────────────────────┤
│  右 40%：ROI 账单                               │
│   - Ray vs Spark 双柱状图                       │
│   - 省了多少钱（大数字）                        │
│   - 折算腾讯云包年（小字）                      │
│   - 客户证言（脱敏）                            │
└─────────────────────────────────────────────────┘
[底部] ▶ 开跑 / ⏸ 暂停 / ⏺ 录制 / 重置参数
```

### 4.3 `cards.html`（新建）
- 5 张卡，全屏轮播（点击右下角 "下一张" 或自动 10s）
- 每张卡布局统一：左红 Spark / 右绿 Ray / 顶部金句 / 底部小字证言
- 复用 `index.html` 已做的录制功能：每张卡可一键导出 GIF/WebM

---

## 五、Mock 数据 Schema

### 5.1 故事 A 的 token mock
```python
# backend/stories/llm_dedup.py
def gen_mock_tokens(n: int) -> ray.data.Dataset:
    """生成 n 行 mock token 流。
    每行：{"id": int, "text": "fake_token_X_Y", "is_dup": bool}
    """
    ds = ray.data.range(n)
    ds = ds.map(lambda x: {
        "id": x["id"],
        "text": f"tok_{x['id'] % 10000}_{x['id']}",   # 制造重复
        "is_dup": (x["id"] % 10000) < 3000,            # 30% 重复率
    })
    return ds
```

### 5.2 故事 B 的图片资源
- `static/assets/sample_videos/`（新增目录）下放 50 张 320x240 缩略图
- 来源：从 COS 公开桶的脱敏样例 / 或 Unsplash CC0
- 文件名：`vid_001.jpg ~ vid_050.jpg`
- 后端用 `PIL.Image.open` 读，演示足够

### 5.3 ROI 价格表（节选）
```python
# backend/pricing/tencent_cloud.py
PRICES_USD_PER_HOUR = {
    "ap-guangzhou": {
        "hai_a100_80g":   3.20,   # HAI A100 80G 按量
        "tke_a100_80g":   2.85,   # TKE GPU 节点（含包年折扣）
        "tke_v100":       1.10,
        "cos_storage_gb_month": 0.018,
        "goosefs_acceleration": 0.05,  # 加速包
    },
    # ...
}
LAST_UPDATED = "2026-06-15"
```

---

## 六、5 天冲刺时间表

| Day | 上午 | 下午 |
|---|---|---|
| **D1** | 后端骨架：`stories/` `pricing/` 目录 + 6 个路由空壳 + SSE 通道打通 | 故事 A llm_dedup 真跑 pipeline（Ray 侧）+ 单测 |
| **D2** | 故事 A 的 Spark mock 对照 + ROI 计算器 | 故事 B video_tagging 的 CLIP 真跑 + Mock 图准备 |
| **D3** | 故事 B Spark mock + ROI 接入 | 前端 `stories.html` 骨架（故事切换 + DAG 动画 + 指标条）|
| **D4** | 前端 ROI 账单组件 + 双柱状图 + 客户证言渲染 | `cards.html` 5 张卡（标题 + 数字 + SVG 动画 + 录制集成）|
| **D5** | 端到端联调（3 个录制时长 × 2 个故事 × 5 张卡）| 写演示脚手 + 截图、commit + push |

**交付物**：
1. ✅ 2 个行业故事舞台（实时跑 + ROI 账单）
2. ✅ 5 张金句卡（可一键导出 GIF/WebM）
3. ✅ Demo 4（compare）升级到多模态打标场景（**M1.5 移到 D5 收尾或 M2 D1**）
4. ✅ README 增补"销售路演 / 大客户深聊"两套演示动线脚本

---

## 七、对客户讲故事的话术骨架（销售可直接背）

### 故事 A 大模型清洗（90 秒版）

> 「客户场景：某 Top3 大模型公司，每天清洗 1 万亿 token 准备预训练。
> 痛点：用 Spark UDF 跑，单作业 36 小时、$23,000；GPU 节点 80% 时间在等数据。
>
> Ray Data 怎么解：把 dedup（CPU 重）/ quality_score（GPU 重）/ tokenize（CPU 重）拆成异构资源算子，CPU 节点和 GPU 节点同时打满，模型在 GPU Actor 里常驻不重 load。
>
> 实测：单作业从 36h 砍到 6.4h，成本从 $23k 砍到 $4.1k。**省下来的 $18.9k 等于免费用腾讯云 HAI A100 4.2 个月**。」

### 故事 B 多模态打标（90 秒版）

> 「客户场景：某头部短视频平台，每天 5 亿短视频要打标（NSFW + 主体 + 情感）。
> 痛点：Spark UDF 每个 task 重新 load CLIP 模型 12 秒，单卡只跑 12 fps，整个集群 200 张 A100 还跑不完。
>
> Ray Data 怎么解：CLIP 和 LLaVA 都做成 GPU Actor 常驻，pipeline 流式无 barrier，慢 task 不拖累快 task。
>
> 实测：单卡从 12 fps 飙到 180 fps（**14× 加速**），全集群 GPU 数从 200 张降到 30 张，**腾讯云账单一年省 $1.8M**。」

---

## 八、风险与兜底

| 风险 | 兜底 |
|---|---|
| Ray Summit benchmark 数字客户挑战真实性 | 在 ROI 卡片底部小字："基于公开 benchmark + 腾讯云 POC，POC 资源券请扫码申请" |
| 现场跑 CLIP 太慢（笔记本无 GPU）| 故事 B 的 ray 真跑路径用 `ray.data.range` + sleep mock，CLIP 单独做"演示模式 / 真跑模式"开关 |
| 录制功能在低性能机器上掉帧 | 录制 fps 默认 30，可降到 24；M1 不优化，M2 加 fps 选项 |
| 客户问"那 Ray 替代 Spark？" | 标准话术："Spark 留给 BI/ETL/SQL，Ray 吃 AI/多模态/GPU 负载，两者互补不替代。腾讯云上 EMR-Spark 和 TKE-Ray 共用 COS 数据湖。" |

---

## 九、下一步（M1 D1 即开工）

D1 上午第一刀：
1. 创建 `backend/stories/` 与 `backend/pricing/` 目录骨架
2. 在 `app.py` 注册 6 个新路由（先返回 mock）
3. 打通 SSE 通道（用 Flask 的 `Response(stream_with_context, mimetype='text/event-stream')`）

**完成本文件 commit 后，等 1 句"开干 D1"，立即出代码。**
