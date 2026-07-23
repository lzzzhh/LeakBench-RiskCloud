---
title: "LeakBench-RiskCloud：云原生大数据多模态风控与数据泄漏治理平台"
subtitle: "项目总体设计文档（架构、数据、特征、模型、工程可靠性与可视化）"
author: "项目方案稿"
date: "2026-07-23"
lang: zh-CN
---

> **文档状态：架构设计提案，不代表已实现结果。**  
> 文中吞吐量、延迟和可用性均为设计目标或验收口径；在完成真实压测前，不应写入简历作为已达成指标。

# 执行摘要

本项目拟将现有 LeakBench-Tab 论文框架扩展为一套云原生、批流一体、多数据集、可审计的风控数据与模型治理平台。平台不以“把所有技术堆在一起”为目标，而是围绕五类真实工程问题组织架构：

1. **多源与多表**：信贷申请、历史贷款、征信、还款、催收、交易和身份文档来自不同表或系统，需要批量聚合和统一时间语义。
2. **历史与实时并存**：历史数据用于训练、回填和 OOT 验证；新事件需要增量计算特征并支持在线评分。
3. **预测时点约束**：所有训练和在线特征必须证明在预测时点可获得，避免贷后字段、未来窗口和标签衍生信息造成数据泄漏。
4. **多模态处理**：图片或扫描文档先经过文档分类、OCR、版面分析和字段抽取，再把可审计的结构化结果写入特征平台；原始图像不直接塞入传统评分卡。
5. **训练与服务一致性**：历史训练集、在线最新特征、模型版本、数据快照和 LeakBench 审计结果必须形成可追踪闭环。

推荐的核心技术栈是：

- **Kafka**：持久事件日志、事件回放、生产者与消费者解耦；
- **Spark**：历史批处理、多表聚合、as-of join、特征回填、WOE/IV/PSI/OOT；
- **Flink**：Event Time、Watermark、状态窗口、迟到数据和实时预测边界审计；
- **Apache Iceberg**：湖仓事实层、快照、版本、Schema/Partition Evolution；
- **Feast + Redis**：特征定义、point-in-time 历史取数、离线到在线物化和低延迟特征服务；
- **Apache Doris**：面向看板和高并发分析的查询服务层，而不是事实数据唯一来源；
- **MLflow**：实验追踪、模型版本、数据与审计 lineage；
- **FastAPI + Kubernetes**：模型和平台 API 服务；
- **React + Prometheus/Grafana**：产品看板与基础设施观测。

项目采用两个相互独立但共享平台的数据产品：

- **结构化风控数据产品**：以 Home Credit 为主，Bank Marketing 和 IEEE-CIS Fraud Detection 用于迁移性及流式验证；
- **多模态文档数据产品**：以 FUNSD、MIDV-500、DocVQA 等公开文档数据验证图片接入、OCR/KIE 和特征化能力。

**不应将没有实体关联的两个公开数据集伪造为同一借款人数据。** 多模态能力通过共享事件合同、Feature Registry、审计和服务接口证明；只有存在真实或明确标注为合成的实体关联时，文档特征才进入同一个信用风险模型。

# 1. 项目目标与边界

## 1.1 最终项目定位

建议项目名称：

**云原生多源信贷风控与模型数据泄漏治理平台**

一句话描述：

> 将结构化信贷表、实时业务事件和文档图像统一转换为带预测时点、可用时间、血缘和版本的特征，通过批流一致的 Feature Store 支持离线训练与在线评分，并在模型进入注册和部署前使用 LeakBench 执行数据泄漏检测、治理策略比较和审计阻断。

## 1.2 主要目标

- 建立数据集 Adapter，使新数据集只需增加字段映射、预测边界和语义组配置，而不修改 LeakBench Core。
- 将静态结构化数据按业务时间事件化，支持可配置 EPS、乱序、重复、迟到和故障重放。
- 建立 Spark 历史回算与 Flink 实时增量两条链路，并通过逐实体、逐特征 hash 证明批流一致性。
- 建立湖仓一体的数据层，绑定每次训练的数据快照、特征定义、代码版本和模型版本。
- 建立离线与在线特征仓库，避免训练—服务偏差和未来信息泄漏。
- 将 LeakBench 变成模型注册前的 Promotion Gate，而不仅是离线论文脚本。
- 提供 React 看板展示数据管道、Feature Store、模型训练、LeakBench 审计和线上监控。

## 1.3 非目标

- 首期不追求真实金融机构的千万级 QPS，也不宣称公开数据具备真实业务规模。
- 不把无关公开数据集强行 join 成“多模态信用评分数据”。
- 不把“实时模型”理解为每到一条贷款申请就立即重新训练。信贷违约标签通常存在较长成熟期，首期应采用**实时特征 + 在线推理 + 周期性离线重训**。
- 不让 Doris、Redis 或看板成为科学事实的唯一来源；正式训练与审计始终绑定 Iceberg 快照和不可变 artifacts。
- 不修改当前 EDBT scientific freeze。大数据平台应作为独立扩展层和 case-study，不混入尚未完成的 RC3/RC2 科学执行。

# 2. 为什么需要大数据架构

## 2.1 不只是“数据量大”

该架构的必要性来自 Volume、Velocity、Variety、Veracity 和 Governance 的组合，而不是单一文件大小。

| 挑战 | 单机脚本的局限 | 平台能力 |
|---|---|---|
| 多表一对多关系 | 容易产生样本膨胀、重复和内存峰值 | Spark 分区聚合、窗口汇总和可重跑的宽表任务 |
| 历史回填 | 每次逻辑变化要重新跑全部数据 | 按日期/快照回填、分区重算、Iceberg 原子快照 |
| 事件持续到达 | CSV 只能反映最终状态 | Kafka 持久事件日志 + Flink 增量状态 |
| 乱序和迟到 | 普通按到达顺序计算会产生错误窗口 | Event Time + Watermark + 迟到侧输出 |
| 训练—服务一致性 | 训练脚本和在线代码各自实现特征 | Feature Registry、point-in-time join、在线物化 |
| 数据泄漏审计 | 只看字段名，无法证明何时可用 | event_time、available_at、prediction_time 三时间合同 |
| 多模态 | 图像、OCR、表格结果散落 | 对象存储 + 文档解析事件 + 结构化特征与 embedding |
| 可复现 | 文件被覆盖后无法重建某次训练 | Iceberg snapshot、MLflow lineage、LeakBench audit receipt |

## 2.2 面试时应如何解释

推荐回答：

> 我引入大数据架构并不是因为 Home Credit 本身必须用集群才能跑，而是因为项目要证明三个能力：第一，多源多表和历史回填可以在数据量增长时水平扩展；第二，同一套特征逻辑能同时支持历史训练和实时评分；第三，所有特征都能够绑定事件时间、可用时间和预测时点，从系统层面防止数据穿越。Spark 负责有界历史数据，Flink 负责无界事件流，Kafka负责持久化和回放，Iceberg负责版本化事实层，Feature Store负责训练和服务一致性。

# 3. 数据集选择与使用策略

## 3.1 选择原则

- 有明确目标变量或可构造的任务；
- 能定义 prediction_time 和 label_time；
- 能展示多表、时间序列、流事件或多模态中的至少一项；
- 许可和下载方式能够被复现实验记录；
- 不要求不同公开数据集之间存在实体级关联；
- 不把无标签的文档图像伪装成信用违约监督数据。

## 3.2 结构化数据集

| 优先级 | 数据集 | 平台用途 | 适合验证的能力 | 主要限制 |
|---|---|---|---|---|
| P0 | Home Credit Default Risk | 主信贷数据产品 | 多表聚合、历史贷款与支付特征、Spark 宽表、评分卡/GBDT | Kaggle 许可需单独遵守；部分业务时间字段需要谨慎定义 |
| P1 | Bank Marketing | LeakBench 跨数据集 Adapter | 数据按日期排序；`duration` 是经典事后信息案例；验证 strict/full/governed | 规模较小，主要用于方法迁移而非压力测试 |
| P1 | IEEE-CIS Fraud Detection | 流式与低延迟风控案例 | transaction + identity 表、交易事件重放、实时欺诈评分、类别不平衡 | 字段匿名、部分时间为相对时间；不能强行做信贷解释 |
| P2 | Lending Club | 信贷自然案例 | 贷前/贷后信息边界、还款与催收字段泄漏、KS/WOE/IV | 数据版本和许可需固定；不同公开副本口径可能不一致 |

Home Credit 的数据由主申请表以及历史征信、历史申请、分期支付、信用卡和 POS/CASH 等一对多表组成，适合用于 Spark 聚合和特征回填案例。[R1][R2] Bank Marketing 包含 45,211 条记录，且官方页面提供按日期排序的完整版本，适合作为可解释的预测时点与事后字段迁移案例。[R3] IEEE-CIS 是交易与身份两表的欺诈检测数据，可用 TransactionID 关联并包含交易、设备和身份变量，适合作为流式评分支线。[R4][R5]

## 3.3 文档与图像数据集

| 优先级 | 数据集 | 模态 | 项目用途 | 是否进入信用风险模型 |
|---|---|---|---|---|
| P0 | FUNSD | 扫描表单图像 + OCR/实体/关系标注 | 表单 OCR、key-value 抽取、版面理解、解析质量监控 | 默认不进入；用于文档特征平台和 KIE 模型 |
| P0 | MIDV-500 | 身份证件视频/图像 + 标注 | KYC 证件检测、透视/模糊质量、字段抽取、事件流上传 | 只在存在实体关联时进入；首期独立任务 |
| P1 | DocVQA | 文档图像 + 问答/OCR | 多模态文档理解与字段查询 API | 默认独立；可生成文档可用性和抽取质量特征 |

FUNSD 包含 199 张完整标注表单、31,485 个单词、9,707 个语义实体和 5,304 个关系，适合作为 KIE 小型可复现实验。[R6] MIDV-500 包含 50 种身份文档的 500 段视频，可用于移动端身份文档检测、文本行识别和字段抽取。[R7] DocVQA 提供约 12K 文档图像和 50K 问题，可用于文档图像理解和查询式字段验证。[R8]

## 3.4 最终推荐组合

第一阶段只实现两条主线：

1. **Home Credit 结构化主线**：完整 Spark 批处理、Kafka 重放、Flink 增量特征、Feature Store、LeakBench、模型和看板。
2. **FUNSD 或 MIDV-500 文档主线**：对象存储上传、解析 worker、文档特征和质量看板。

Bank Marketing 作为第一个 Adapter 迁移验收；IEEE-CIS 作为第二阶段实时欺诈支线。这样既能证明通用性，又避免首期同时维护四套复杂数据逻辑。

# 4. 技术选型与组件工作原理

## 4.1 Kafka：为什么不是普通消息队列

Kafka 的核心不是“把消息发给下游”，而是**分区化、可保留、可回放的提交日志**。事件包含 key、value、timestamp 和 headers；Topic 被划分为多个 Partition，每个 Partition 内有序，消费者通过 Consumer Group 分摊 Partition。[R9][R10]

在本项目中：

- `key = customer_id` 用于客户级历史窗口；
- `key = application_id` 用于申请级特征和预测；
- 每个逻辑下游使用独立 consumer group，例如 `flink-feature-builder`、`document-parser`、`audit-materializer`；
- Kafka 保留期允许故障后从旧 offset 重放，也允许使用同一历史事件做不同版本算法的重算；
- Partition 数决定同一 consumer group 的最大并行消费上限，不能简单通过无限增加消费者扩容。

Kafka 的 exactly-once 需要理解为生产、消费 offset 和输出事务的组合；不是任意外部系统都自动获得端到端 exactly-once。[R11]

## 4.2 Spark：离线批处理引擎

Spark 负责有界、历史、吞吐优先的工作：

- 多张 parquet/CSV/Iceberg 表读取；
- 一对多聚合；
- 训练窗口和 OOT 窗口构造；
- point-in-time/as-of join；
- WOE、IV、PSI、KS、样本统计；
- 历史回填和批量评分。

面试需要理解：一个 Spark Job 根据宽依赖被切分为多个 Stage，Stage 内由 Task 处理 Partition；`groupBy`、大表 join 和 repartition 会产生 Shuffle。工程优化重点是减少无效 Shuffle、避免数据倾斜、使用列裁剪和分区裁剪、控制小文件、选择合适的广播 join 和 executor 资源。

本项目不使用 Spark Structured Streaming 作为主实时引擎。虽然 Structured Streaming 能以增量表模型执行流式计算并通过 checkpoint 提供容错，但本项目选择 Flink，是为了突出更原生的 Event Time、长状态和低延迟连续处理；Spark 保持批处理职责边界。[R12]

## 4.3 Flink：实时状态与业务时间

Flink 的核心能力不是“更快地跑 SQL”，而是对无界事件执行带状态、事件时间驱动的计算。

- **Event Time**：事件在业务世界发生的时间；
- **Processing Time**：机器实际处理事件的时间；
- **Watermark**：系统对事件时间进度的估计，用于决定窗口何时可以计算；
- **Keyed State**：按客户或贷款保存历史状态；
- **Checkpoint**：状态与源位置的一致快照，用于故障恢复；
- **Savepoint**：用于有计划升级、迁移和版本切换的人工快照。

Flink 的 WatermarkStrategy 将 timestamp assigner 与 watermark generator 组合，并可按 Kafka Partition 生成水位线；Watermark 过于激进会丢弃正常迟到数据，过于保守会提高延迟并扩大状态。[R13]

Checkpoint 需要可回放的持久数据源和持久状态存储。Flink 的 exactly-once 首先表示事件对托管状态的影响一次；端到端 exactly-once 还要求源可回放、Sink 事务化或幂等。[R14][R15]

## 4.4 Iceberg：湖仓事实层

Iceberg 在对象存储文件之上维护表级元数据、快照和提交语义。每次写入形成新 Snapshot，可用于 time travel、回滚和训练数据版本绑定；Schema、Partition 和 Sort Order 可以演进而不要求重写全部旧数据。[R16][R17]

项目中 Iceberg 是**系统事实来源**：

- Bronze：不可变原始文件和事件；
- Silver：标准化、去重、时间字段修复后的实体与事件；
- Gold：特征、Prediction Point、训练样本和评分；
- Audit：数据质量、边界违规、LeakBench 结果和 lineage。

必须定期执行小文件合并、Snapshot 过期、孤儿文件清理和 manifest 重写；否则湖仓元数据会持续膨胀。[R17]

## 4.5 Apache Doris：查询服务层

Doris 用于低延迟、高并发看板和交互分析：

- 查询 Iceberg 外表；
- 将常用指标物化为 Doris 内部表；
- 使用同步或异步物化视图加速固定聚合；
- 服务 React 看板的秒级筛选和钻取。

Doris 不是训练数据的唯一事实来源。训练仍从绑定 Snapshot 的 Iceberg/Feature Store 读取，防止看板物化延迟影响科学结果。Doris 官方将实时分析、Kafka/CDC 导入、Iceberg/Hudi/Delta 查询和物化视图作为核心场景。[R18][R19]

## 4.6 Feature Store：Feast + Redis

Feature Store 分为三个概念：

- **Feature Registry**：特征定义、实体、owner、版本、TTL、数据源和服务组合；
- **Offline Store**：保存历史时间序列特征，用于训练集和回填；
- **Online Store**：每个实体保存最新可服务特征，用于低延迟推理。

Feast 可以从批处理系统生成的特征中构造 point-in-time correct 历史训练集，并将离线特征物化到在线存储；流特征可以通过 Push API 写入。[R20][R21]

本项目建议：

- Offline：Iceberg Gold 表；首期可由 Spark 导出训练快照供 Feast/Spark historical retrieval；
- Online：Redis；
- Registry：Feast definitions + Git；
- Feature Server：Feast/FastAPI；
- Embedding：不直接塞入 Redis，使用 pgvector/Milvus/OpenSearch 等向量存储；Redis 只存下游模型需要的低维或标量特征。

## 4.7 MLflow 与 Kubernetes

MLflow Tracking 记录参数、指标、数据快照和 artifacts；Model Registry 管理版本、alias、标签与 lineage。[R22][R23]

Kubernetes Deployment 负责在线服务副本、滚动更新和回滚；RollingUpdate 通过 `maxUnavailable` 和 `maxSurge` 控制新旧版本替换。[R24]

模型版本必须带以下标签：

```text
iceberg_snapshot_id
feature_service_version
prediction_boundary_version
adapter_version
leakbench_audit_status
leakbench_audit_receipt_sha
training_code_sha
model_signature
```

# 5. 总体架构

![总体架构](diagrams/overall_doc_trim.png){width=95%}

架构按职责分为六层：

1. 数据源与事件化；
2. Kafka/对象存储接入；
3. Spark、Flink 与多模态计算；
4. Iceberg、Feast、Redis、Doris 和向量存储；
5. LeakBench、训练、MLflow 与模型部署；
6. React 看板和可观测系统。

重要原则：**同一份业务事实可以同时以文件和事件存在，但必须使用 event_id、source_version、event_time 和 lineage_hash 证明它们是同一事实，不能把两条链路当成两个独立真相。**

# 6. 统一数据合同

## 6.1 事件合同

```json
{
  "dataset_id": "home_credit",
  "event_id": "sha256(...) ",
  "entity_type": "loan_application",
  "entity_id": "SK_ID_CURR:100001",
  "customer_id": "customer:...",
  "event_type": "bureau_snapshot",
  "event_time": "2018-01-15T10:30:00Z",
  "available_at": "2018-01-15T10:31:10Z",
  "ingested_at": "2026-07-23T08:00:01Z",
  "source_system": "home_credit_adapter",
  "schema_version": 1,
  "payload_uri": "s3://.../event.json",
  "payload_sha256": "..."
}
```

三个时间字段必须区分：

- `event_time`：事实发生时间；
- `available_at`：该事实在业务系统中可供模型使用的时间；
- `ingested_at`：平台接收到事件的时间。

数据泄漏判断主要比较 `available_at` 与 `prediction_time`，不能只比较事件发生时间。

## 6.2 Prediction Point 合同

```text
prediction_id
entity_id
prediction_time
label
label_time
split            # train / validation / oot / online
snapshot_id
boundary_version
```

每次训练必须先生成 Prediction Point，再执行 point-in-time join。不能先把所有表聚合到最终状态，再随机切训练集。

## 6.3 Feature Catalog 合同

| 字段 | 含义 |
|---|---|
| feature_id / feature_name | 稳定标识与可读名称 |
| entity_type | customer、application、transaction、document |
| feature_group | 申请、征信、还款、催收、文档质量等 |
| source_system | 原始来源 |
| event_time_rule | 如何确定事实发生时间 |
| availability_rule | 何时可供预测使用 |
| stage | pre_application / application / decision / post_decision / post_outcome / label_derived |
| online_available | 是否需要在线服务 |
| ttl | 在线值有效期 |
| owner | 负责人或模块 |
| version | 定义版本 |
| leakage_risk | none / temporal / post_outcome / label_derived / unknown |
| semantic_group_id | LeakBench 语义组 |
| cost_unit | 治理成本单位 |
| lineage_expression | 计算逻辑或 SQL/hash |

## 6.4 文档解析合同

```text
document_id
entity_id                # 可为空，未关联数据集时不强行绑定
object_uri
content_sha256
document_type
ocr_text_uri
layout_json_uri
extracted_fields_json
ocr_confidence
field_coverage
image_quality_score
tamper_signal
model_version
processed_at
```

如果图像数据集与信贷数据没有共同实体，`entity_id` 保持为空或使用文档数据集自己的实体；可以训练文档模型和展示特征平台，但不能声称提高了 Home Credit 违约预测。

# 7. 数据事件化与实时模拟

静态表转事件流时，不是简单逐行 sleep：

1. 为每类表定义业务事件类型；
2. 从字段推导或合成 `event_time`；
3. 定义 `available_at`；
4. 使用稳定 `event_id` 保证重复重放可幂等；
5. 按 `entity_id` 作为 Kafka key；
6. 支持可配置 EPS、乱序比例、迟到分布、重复比例和故障注入；
7. 记录 replay manifest，绑定输入文件 SHA、随机种子和重放配置。

建议模拟参数：

```yaml
replay:
  source_snapshot: home_credit_v1
  rate_eps: 500
  speedup: 86400        # 1秒模拟1天，仅为示例
  out_of_order_ratio: 0.05
  max_lateness: 10m
  duplicate_ratio: 0.001
  seed: 20260723
```

在简历中只能写“构建可配置事件重放器并完成某规模压测”，不能把设计 EPS 当作实测吞吐。

# 8. 离线 Spark Pipeline

离线链路负责历史训练、回填、规则拟合和批量评分。

```text
Raw files
→ Bronze ingestion
→ Silver cleaning/deduplication
→ Prediction Points
→ As-of feature aggregation
→ WOE/IV rules fitted on train only
→ Strict / Full feature views
→ LeakBench governance
→ Governed feature view
→ OOT datasets and model training
```

## 8.1 关键工程规则

- 每个任务使用 `run_id`、input snapshot 和 code SHA；
- 输出按 `event_date`、`dataset_id`、`feature_version` 分区；
- 重跑使用 Iceberg overwrite by partition 或 merge，而不是 append 造成重复；
- 所有分箱、标准化和目标编码规则只在训练窗口拟合；
- 验证/OOT 只应用冻结规则；
- 大表 join 先做键分布和倾斜检查；
- 对热 key 使用 salting 或分段聚合；
- 在宽表前尽量完成子表聚合，避免行数爆炸；
- 每次输出执行 row count、主键、null rate、时间边界和 lineage closure。

## 8.2 WOE/IV/KS/PSI

- WOE/IV 规则表保存训练快照、bin 边界和版本；
- 高 IV 不等于合法特征。贷后字段可能 IV 极高，但仍应由预测时点规则阻断；
- KS、AUC、Lift 对 strict/full/governed 三类模型分别计算；
- PSI 用于特征和模型分数的时间稳定性；
- 建议增加 `KS inflation = KS_full - KS_strict` 作为风控 case-study 辅助指标，但不能替换论文冻结指标。

# 9. 实时 Kafka/Flink Pipeline

![批流汇合](diagrams/batch_stream_trim.png){width=84%}

## 9.1 实时计算步骤

```text
Kafka event
→ schema validation
→ event-id deduplication
→ timestamp extraction
→ watermark assignment
→ keyBy(customer/application)
→ stateful windows and temporal joins
→ prediction-boundary check
→ online feature upsert
→ Iceberg audit append
→ optional scoring request
```

## 9.2 迟到数据策略

迟到数据不能只有“丢弃”或“无限等待”两个选项。

- Watermark 内：正常更新窗口；
- 超过 Watermark 但仍在 allowed lateness：修正结果，并产生 correction event；
- 超过最大容忍：写入 `late-event-dlq`，等待批处理回填；
- 已经完成的在线预测不静默覆盖，记录 feature correction 与 prediction lineage；
- 历史训练快照由 Spark 最终回算结果负责，保证可重建。

## 9.3 实时训练的正确表述

信用违约模型通常标签成熟较慢，因此首期不做“每条事件触发一次在线重训”。推荐：

- 实时：特征更新、规则判断、在线推理、监控；
- 每日/每周：数据质量汇总和批量评分；
- 每月/按标签成熟度：离线重训；
- 可选：在 IEEE-CIS 欺诈支线验证增量学习或短周期 challenger，但与信用违约主模型分开。

# 10. 多模态文档处理 Pipeline

```text
Upload image/PDF
→ Object Storage
→ document-uploaded event
→ malware/type/size validation
→ document classifier
→ OCR
→ layout analysis / KIE
→ field validation and consistency rules
→ feature extraction
→ Iceberg document tables
→ scalar features to Feature Store
→ embeddings to Vector Store
```

## 10.1 推荐特征

- 文档类型；
- OCR 平均/最低置信度；
- 必填字段覆盖率；
- 姓名、证件号、日期的格式合法性；
- 文档过期标识；
- 模糊、反光、裁剪和透视质量；
- 多页字段一致性；
- 文档字段与申请字段一致性（只有存在实体关联时）；
- 可疑篡改信号；
- 文档 embedding 或版面 embedding。

## 10.2 存储原则

- 原始文件：对象存储；
- OCR 和版面 JSON：Iceberg/对象存储；
- 标量特征：Feature Store；
- 高维 embedding：向量存储；
- 看板缩略图：受控缓存；
- PII：脱敏、加密、最小权限和访问审计。

# 11. 湖仓与特征仓库设计

## 11.1 Iceberg 表分层

```text
bronze.raw_file_manifest
bronze.raw_events
bronze.raw_documents

silver.normalized_events
silver.loan_application
silver.bureau_history
silver.repayment_events
silver.document_parse_results

gold.prediction_points
gold.offline_feature_values
gold.strict_training_view
gold.full_training_view
gold.governed_training_view
gold.batch_scores

audit.feature_catalog
audit.feature_lineage
audit.boundary_violations
audit.batch_stream_parity
audit.leakbench_runs
audit.data_quality_results
```

## 11.2 Offline Feature 表

推荐长表作为权威存储，宽表作为训练物化结果：

```text
dataset_id
entity_id
feature_id
feature_value
feature_dtype
event_timestamp
available_at
created_timestamp
feature_version
source_snapshot_id
lineage_hash
quality_status
```

长表便于版本和多数据集治理；宽表由 Feature Service 或训练快照任务按模型需求生成。

## 11.3 Online Store Key

```text
{feature_service}:{entity_type}:{entity_id}
```

Value 至少包括：

```text
feature_name -> value
feature_version
source_event_time
available_at
materialized_at
lineage_hash
```

在线 Store 只保存最新值，不承担历史回溯。历史训练必须读取 Offline Store。[R21]

## 11.4 Batch-Stream Parity

对同一事件时间范围：

```text
Spark historical recomputation
vs.
Flink incremental output
```

比较：

- entity_id；
- prediction_time；
- feature_id；
- value；
- max source_event_time；
- lineage_hash。

验收规则：

- 离散/整数特征必须完全一致；
- 浮点特征按明确容差；
- 缺失、额外和边界差异单独统计；
- parity 未通过的 Feature Service 不得推广到在线模型。

# 12. 模型训练、部署与生命周期

## 12.1 离线训练

云端 Pipeline 生成不可变训练快照。用户可以：

1. 在云端 Spark/容器训练；或
2. 通过短期签名 URL 下载脱敏 Parquet 到本地训练。

本地训练仍将参数、指标和模型 artifacts 写入远端 MLflow。训练任务必须声明：

```text
training_snapshot_id
feature_service
feature_service_version
prediction_boundary_version
leakbench_run_id
code_sha
random_seed
split_definition
```

## 12.2 在线推理

```text
request
→ request schema validation
→ online feature retrieval
→ freshness and boundary check
→ model inference
→ decision/risk score
→ prediction event to Kafka
→ async monitoring and label feedback
```

在线服务使用 FastAPI 容器，Kubernetes 负责副本、健康检查和滚动升级。部署模式：

- Champion：正式版本；
- Challenger：少量流量或 shadow；
- Shadow：读取同一请求但不影响决策；
- Canary：按流量比例逐步推广；
- Rollback：使用 MLflow alias 和 Kubernetes rollout 回退。

## 12.3 批量评分

Spark 读取绑定 Snapshot 的 Gold 特征，输出分区化批量评分表。批量评分适合：

- 存量客户风险复评；
- OOT 评估；
- 月度策略模拟；
- 模型监控基准；
- LeakBench 全量实验。

# 13. LeakBench 如何嵌入训练过程

![LeakBench 模型门禁](diagrams/model_gate_doc_trim.png){width=58%}

## 13.1 训练前

- 从 Feature Catalog 读取 stage、available_at 规则、语义组和成本；
- 生成 Strict View；
- 生成 Full View，包含明确标注的泄漏候选；
- 冻结数据快照和 feature list。

## 13.2 训练中

- 训练 strict baseline；
- 训练 full/leaky baseline；
- 计算 AUC/KS inflation；
- 运行随机、MI、IV、LR/RF 排序和语义组治理；
- 生成 Governed View；
- 计算 SDR、Leakage Recall、Legitimate Retention、治理成本和 overcorrection；
- 输出 audit receipt。

## 13.3 注册前 Promotion Gate

只有同时满足以下条件，模型才能进入 MLflow `candidate` 或 `champion`：

- 数据快照、Feature Service 和代码均绑定；
- strict/full/governed 结果完整；
- 不存在未授权 post-outcome/label-derived 特征；
- 批流一致性通过；
- OOT、KS、PSI 和策略阈值通过；
- LeakBench receipt 已验证；
- 模型签名和依赖环境完整。

## 13.4 在线运行时

Flink 不重新执行整套科学实验，而是使用同一 Feature Catalog 做轻量门禁：

```text
available_at > prediction_time
→ block feature and emit violation

feature version not in deployed Feature Service
→ reject request or fallback

online/offline parity status != PASS
→ stop promotion
```

# 14. 工程可靠性设计

## 14.1 交付语义与幂等

事件必须包含稳定 `event_id`。Sink 使用以下方式之一：

- 支持事务的 Iceberg/Flink Sink；
- 主键 upsert；
- `event_id + feature_version` 唯一约束；
- 先写 staging，再原子发布；
- 外部副作用使用 outbox 或幂等键。

“Exactly once”不能只写在文档中，必须说明 source、state、sink 和外部副作用各自的保证。[R14][R15]

## 14.2 错误恢复矩阵

| 故障 | 检测 | 恢复 | 一致性要求 |
|---|---|---|---|
| Kafka Broker/网络故障 | broker/ISR、producer error、lag | 重试、复制副本、从 offset 继续 | 不丢 committed event；重复由 event_id 处理 |
| Flink Task 失败 | restart、checkpoint failure | 从最近完成 checkpoint 恢复 | 状态与 source position一致 |
| Flink 状态过大/背压 | backpressure、checkpoint duration、state size | 扩容、unaligned checkpoint、TTL、拆分热点 key | 不以丢事件换延迟 |
| Spark Job 失败 | Airflow task failure、Spark event log | 幂等重跑同一分区/快照 | 不 append 重复结果 |
| Iceberg commit conflict | commit exception | 重新读取 metadata 后重试 | 不覆盖他人提交 |
| OCR Worker 失败 | job timeout、parse status | DLQ、重试、人工复核 | 原图不丢，结果可重放 |
| Redis 不可用 | latency/error rate | 降级、缓存、熔断；关键业务 fail closed | 不使用过期未知特征静默评分 |
| 模型新版本异常 | readiness、error/latency/drift | 停止 rollout、切回 champion | 预测事件记录实际 model version |
| 看板失败 | API/前端监控 | 看板重启 | 不影响数据处理和模型事实层 |

## 14.3 Dead Letter Queue

建议 DLQ Topic：

```text
dlq.schema-invalid
dlq.late-event
dlq.document-parse
dlq.feature-boundary
dlq.online-materialization
```

DLQ 不是垃圾桶。每条记录要包含：

- 原始 topic/partition/offset；
- event_id；
- error code；
- schema version；
- retry count；
- first/last failure time；
- payload URI；
- replay status。

## 14.4 Schema Evolution

- Kafka 事件使用版本化 Avro/Protobuf/JSON Schema；
- 默认 backward compatibility；
- 删除或重命名字段先增加新字段并双写；
- Feature Catalog 中版本变化必须触发 batch-stream parity；
- Iceberg Schema Evolution 不等于业务语义自动兼容，feature definition 仍需新版本。[R16]

## 14.5 Backpressure

Backpressure 表示下游处理速度跟不上上游。需要监控：

- Kafka consumer lag；
- Flink busy/backpressured time；
- checkpoint alignment 和 duration；
- state size；
- sink commit latency；
- Redis/Doris 写入耗时。

处理顺序：先定位瓶颈，再决定增加并行度、重分区、异步 IO、批量写入或降低每条事件计算量。不能只提高 Kafka Partition 而忽略下游状态和 Sink。

## 14.6 Iceberg 运维

- 定期 compact 小文件；
- expire snapshots，但保留被模型或审计绑定的 Snapshot；
- 删除 orphan files；
- 重写 manifests；
- 监控 commit 冲突和 metadata 大小。[R17]

## 14.7 安全与合规

- 对象存储、Kafka、数据库和网络传输加密；
- PII tokenization；
- 文档原图最小权限；
- IAM/RBAC；
- 审计日志；
- 本地训练只获取脱敏且绑定 Snapshot 的训练快照；
- 使用短期签名 URL、VPN/PrivateLink；
- 模型日志禁止记录完整身份证号或敏感 OCR 文本。

# 15. 可观测性、延迟与 SLO

## 15.1 延迟拆分

不要只写“实时延迟”。至少区分：

```text
ingestion_latency = ingested_at - event_time
processing_latency = feature_written_at - ingested_at
feature_freshness = scoring_time - source_event_time
inference_latency = response_time - request_time
end_to_end_latency = response_time - event_time
```

## 15.2 设计目标（未实测）

| 指标 | MVP 目标 | 说明 |
|---|---:|---|
| Kafka event ingestion p95 | < 2 s | 从 replay producer 到 broker确认 |
| Flink feature processing p95 | < 5 s | 不含超过 Watermark 的迟到事件 |
| Online feature retrieval p95 | < 20 ms | Redis/Feature Server 内网 |
| Model inference p95 | < 100 ms | 结构化树模型，不含图像解析 |
| Document parsing | 异步分钟级 | 不阻塞申请主评分链路 |
| Batch-stream deterministic parity | 100% | 离散与整数特征 |
| Full-B1 scientific artifacts | 100% validator closure | 继续遵守现有 R10 系列合同 |

以上是验收目标，不是简历结果。真实结果应由 benchmark receipt 绑定环境、数据规模和 commit。

## 15.3 观测工具

Prometheus 保存带时间戳和标签的数值时序指标；Grafana 用于交互看板和告警。[R25][R26] OpenTelemetry 用于 traces、metrics 和 logs 的统一上下文传播。[R27]

重点指标：

- Kafka：producer error、under-replicated partitions、consumer lag；
- Flink：checkpoint success/duration、restart、backpressure、late records；
- Spark：job/stage duration、shuffle read/write、spill、skew；
- Iceberg：file count、average file size、snapshot count、commit latency；
- Feature Store：freshness、missing rate、online/offline mismatch、Redis p95；
- Model：AUC/KS/PSI、score distribution、prediction latency、error rate；
- LeakBench：strict/full gap、governed recovery、overcorrection、policy cost；
- OCR：parse success、confidence、field coverage、manual review rate。

# 16. React 可视化看板

## 16.1 架构原则

React 不应直接连接 Kafka。推荐：

```text
Kafka / Flink / Spark / MLflow / Feature Store / Prometheus
→ Materializer or API Backend
→ Doris / PostgreSQL / Prometheus APIs
→ FastAPI BFF
→ WebSocket/SSE + REST
→ React
```

看板只读或通过受控 API 触发任务；不能绕过 Airflow、Feature Registry 和模型 Promotion Gate。

## 16.2 页面设计

### 数据管道页

- Kafka Topic、Partition、lag、吞吐；
- Flink job、checkpoint、watermark、late events；
- Spark/Airflow DAG 进度；
- DLQ 和重放状态。

### 湖仓与表页

- Bronze/Silver/Gold/Audit 表；
- Iceberg Snapshot 历史；
- 分区、文件数、大小；
- Schema Evolution；
- 采样数据与 lineage。

### Feature Store 页

- Feature Service、实体和特征定义；
- owner、version、TTL、freshness；
- offline/online 值对比；
- batch-stream parity；
- leakage risk 和 semantic group。

### 训练与模型页

- MLflow Run；
- 参数、指标、数据 Snapshot；
- 训练日志和阶段状态；
- Model Registry、champion/challenger；
- canary/rollback 状态。

### LeakBench 审计页

- strict/full/governed AUC、KS、Lift；
- SDR、Leakage Recall、Legitimate Retention；
- 预算、策略和成本合同；
- 边界违规字段；
- audit receipt 与允许 claim。

### 多模态文档页

- 上传任务；
- 文档分类；
- OCR/KIE 结果；
- 置信度、字段覆盖、图像质量；
- 失败与人工复核队列。

# 17. 云端 Pipeline 与本地训练

## 17.1 云中立逻辑部署

```text
Object Storage     → S3-compatible
Kafka              → Managed Kafka or Kubernetes
Spark              → Managed Spark or Kubernetes
Flink              → Managed Flink or Kubernetes
Iceberg Catalog    → REST/Glue/Nessie
Feature Registry   → Feast + Git/PostgreSQL
Online Store       → Redis
Analytics Serving  → Doris
Model Tracking     → MLflow + PostgreSQL + Object Storage
API/Model Service  → Kubernetes
Dashboard          → React CDN + FastAPI
```

## 17.2 AWS 参考映射

- S3：对象存储和 Iceberg 数据；
- MSK Serverless：兼容 Kafka 的托管流平台，自动配置和扩缩容量；[R28]
- EMR Serverless：Spark 批处理；[R29]
- Managed Service for Apache Flink：托管 Flink 流计算；[R30]
- Glue Catalog：Iceberg Catalog；
- ElastiCache for Redis：在线特征；
- EKS：模型/API 服务；
- RDS PostgreSQL：MLflow、Feature Registry 和平台元数据；
- CloudWatch/Prometheus/Grafana：监控。

## 17.3 本地训练流程

```text
React 发起训练快照请求
→ FastAPI 创建 training_request
→ Airflow 触发 Spark point-in-time snapshot
→ Iceberg commit + manifest
→ 生成受控 Parquet export
→ 返回短期签名下载地址
→ 本地 Trainer 下载并校验 SHA
→ 本地训练
→ 参数、指标和模型上传远端 MLflow
→ LeakBench Gate
→ 人工批准后注册/部署
```

本地训练客户端必须验证：

- manifest SHA；
- 数据文件 SHA；
- row count；
- feature list；
- snapshot id；
- expiration time；
- PII 脱敏状态。

# 18. 编排与自动化

Airflow DAG 负责任务依赖、调度、重试、超时和 backfill。Airflow 的 Dag 定义任务、顺序、条件、回调和调度；Scheduler 根据依赖触发任务。[R31][R32]

建议 DAG：

```text
home_credit_daily_ingest
home_credit_feature_backfill
feature_parity_audit
woe_iv_rule_fit
leakbench_governance_audit
model_training_and_registration
model_batch_scoring
iceberg_maintenance
late_event_reconciliation
document_parse_reprocessing
```

流式 Flink Job 不由 Airflow 每分钟启动；Airflow 管理其发布、savepoint、升级和健康检查。

# 19. 分阶段实施路线

## Phase 0：合同与仓库隔离

- 新增 `platform/` 和 `case_studies/`；
- 冻结事件、Prediction Point、Feature Catalog、文档结果合同；
- 保持 LeakBench scientific core 不变。

验收：Schema tests、Adapter interface tests、禁止修改 scientific freeze。

## Phase 1：Home Credit Spark 垂直链路

- Bronze/Silver/Gold；
- 多表聚合；
- Prediction Point；
- as-of join；
- WOE/IV；
- strict/full views；
- Iceberg snapshots。

验收：主键与行数闭包、时间边界、可重跑、同配置 byte/data equivalent。

## Phase 2：Feature Store 与本地训练

- Feature Registry；
- Offline/Online Store；
- training snapshot API；
- MLflow；
- LR/CatBoost/LightGBM；
- OOT/KS/PSI。

验收：point-in-time correct retrieval；模型绑定 snapshot；本地下载 SHA 校验。

## Phase 3：Kafka/Flink 实时链路

- event replayer；
- Kafka topics；
- Flink windows；
- online feature materialization；
- late data、DLQ、checkpoint；
- batch-stream parity。

验收：故障恢复、重放、迟到修正、0 silent data loss。

## Phase 4：LeakBench Promotion Gate

- Adapter 输出 strict/full/governed；
- policy/cost/semantic mapping；
- audit receipt；
- MLflow promotion blocking。

验收：泄漏字段进入候选时被检测；audit 未通过不能注册为 champion。

## Phase 5：多模态文档支线

- FUNSD/MIDV-500；
- 上传、OCR/KIE、质量特征；
- Vector Store；
- 文档看板。

验收：解析可重放、结果绑定原图 SHA、无关联数据不进入信用模型。

## Phase 6：React 看板

- 数据、特征、训练、模型、LeakBench、文档、运维页面；
- WebSocket/SSE；
- RBAC。

## Phase 7：云端短时部署与压测

- Terraform；
- AWS reference stack；
- 压测、故障注入、成本记录；
- 资源销毁。

# 20. 测试与验收体系

## 20.1 数据合同测试

- Schema compatibility；
- required field/type；
- event_id uniqueness；
- event_time <= available_at；
- Prediction Point uniqueness；
- Feature Catalog versioning。

## 20.2 Batch tests

- row-count closure；
- one-to-many aggregation correctness；
- rerun idempotency；
- skew fixture；
- partition overwrite；
- WOE rules train-only。

## 20.3 Stream tests

- in-order/out-of-order；
- duplicate；
- late event；
- restart from checkpoint；
- Kafka replay；
- Sink idempotency；
- backpressure；
- DLQ replay。

## 20.4 Feature Store tests

- point-in-time correctness；
- online freshness；
- TTL；
- missing value behavior；
- batch-stream parity；
- feature version incompatibility。

## 20.5 Model/LeakBench tests

- strict/full separation；
- post-outcome field injection；
- governance policy reproducibility；
- audit receipt binding；
- model promotion fail closed；
- training-serving skew。

## 20.6 多模态测试

- image SHA binding；
- corrupted image；
- low confidence；
- missing pages；
- OCR/KIE version change；
- PII redaction；
- parse retry/DLQ。

# 21. 常见面试问题与回答框架

## 为什么同时使用 Spark 和 Flink？

Spark 负责历史有界数据、复杂 join、批量回填和训练；Flink 负责无界事件、状态窗口、Event Time 和低延迟增量。两者不是重复计算，而是共享 Feature Catalog 并通过 parity audit 保证一致。

## Kafka 的 Partition 有什么影响？

Partition 是顺序和并行度的单位。相同 key 进入同一 Partition 可以保持实体内顺序；一个 consumer group 内每个 Partition 同时只由一个 consumer 实例消费，因此消费者数量超过 Partition 数不会继续增加并行度。[R10]

## Watermark 是什么？

Watermark 是对事件时间进度的估计，用于决定窗口何时关闭。它不是“当前时间”，而是在可容忍乱序条件下认为更早事件大概率已经到齐的边界。过小延迟会丢迟到事件，过大则增加状态和结果等待时间。[R13]

## Checkpoint 与 Savepoint 区别？

Checkpoint 主要用于自动故障恢复，由系统周期性创建；Savepoint 通常由运维显式触发，用于升级、迁移和有计划的版本切换。

## Exactly-once 是否意味着每条消息只执行一次？

不一定。Flink 的 exactly-once 首先是状态效果一次。端到端保证还要求 source 可回放、Sink 事务化或幂等；外部 HTTP、邮件或非事务数据库副作用仍需 idempotency key 或 outbox。[R14][R15]

## 为什么用湖仓而不是只用数仓？

对象存储适合保存原始文件、图片和大规模历史数据；Iceberg 增加表级快照和演进；Doris负责交互查询。湖仓保留开放文件与多引擎，Doris提供面向看板的低延迟服务，两者职责不同。

## Feature Store 解决什么？

Feature Store 不只是存特征。它管理特征定义、历史 point-in-time retrieval、在线最新值、版本、TTL 和 Feature Service，从而降低训练—服务偏差和未来信息泄漏。[R20][R21]

## 为什么不做真正实时训练？

信用违约标签需要成熟期，立即重训会使用不完整标签并导致偏差。实时部分应是特征和评分，训练按标签成熟度周期运行；欺诈支线可以单独探索增量学习。

## Doris 与 Iceberg 谁是数据源？

Iceberg 是事实和版本来源；Doris 是高并发查询和看板服务层。模型训练不依赖可能存在刷新延迟的物化视图。

# 22. 简历表述建议

项目完成并获得真实 benchmark 后，可写为：

**云原生多源信贷风控与模型数据泄漏治理平台**

- 基于 Home Credit 等多源信贷数据构建 Spark 历史回算与 Kafka/Flink 实时特征双链路，通过 Event Time、Watermark 和 point-in-time join 统一贷款申请、征信、还款及催收数据的预测时点口径。
- 使用 Iceberg 建设 Bronze/Silver/Gold/Audit 湖仓分层，并以 Feast + Redis 构建离线/在线特征平台；通过逐实体、逐特征 parity audit 控制训练—服务偏差和未来信息穿越。
- 将 LeakBench strict/full/governed 框架接入模型注册门禁，在统一治理成本下比较随机、MI/IV 和语义组策略，并结合 AUC、KS、Lift、PSI、SDR 与合法特征保留率评估模型风险。
- 构建文档图像异步解析链路，将 OCR、字段覆盖、图像质量和一致性结果写入 Feature Store；基于 MLflow、FastAPI、Kubernetes 和 React 实现模型版本、在线评分及数据/模型监控。

必须用真实压测结果替换泛化描述，例如处理记录数、实际 p95 延迟、checkpoint 恢复时间、parity 结果和模型指标；未完成前不要填写数字。

# 23. 主要风险与控制

| 风险 | 控制 |
|---|---|
| 技术栈过多导致无法完成 | 分阶段交付，先完成 Home Credit Spark 垂直链路 |
| 为多模态伪造实体关联 | 两个数据产品独立；无关联时只共享平台合同 |
| 实时训练叙事不合理 | 改为实时特征/推理 + 标签成熟后的离线重训 |
| Watermark 配置错误 | 记录迟到分布，建立 replay 和 correction tests |
| Exactly-once 被过度宣称 | 分层声明 source/state/sink/external side effect 语义 |
| Feature Store 只是 Redis | 同时实现 Registry、Offline、Online、PIT retrieval 和 parity |
| 看板成为事实来源 | 看板只读 Doris/Prometheus/MLflow，科学结果绑定 Iceberg artifacts |
| 云成本不可控 | 本地 Compose 完成后短时部署，Terraform 一键销毁 |
| 与论文 freeze 冲突 | 新增 platform/case_studies，不改当前 scientific core |

# 24. 推荐仓库结构

```text
LeakBench-Tab/
├── src/leakbench/                  # 当前论文核心，保持冻结
├── platform/
│   ├── contracts/
│   ├── adapters/
│   │   ├── home_credit/
│   │   ├── bank_marketing/
│   │   ├── ieee_cis/
│   │   └── documents/
│   ├── kafka/
│   ├── spark/
│   ├── flink/
│   ├── lakehouse/
│   ├── feature_store/
│   ├── multimodal/
│   ├── orchestration/
│   ├── serving/
│   ├── dashboard/
│   ├── observability/
│   └── infra/
├── case_studies/
│   ├── home_credit/
│   ├── bank_marketing/
│   └── document_kyc/
├── tests/platform/
├── benchmarks/
└── docs/riskcloud/
```

# 25. 近期唯一建议动作

先不要搭全套云资源。下一步只完成一个可验收垂直切片：

```text
Home Credit 原始多表
→ Spark Bronze/Silver
→ Prediction Point
→ as-of features
→ Iceberg Gold strict/full views
→ Feature Catalog
→ LeakBench Adapter
→ 本地 LR/CatBoost 训练
→ MLflow 记录
```

该切片通过后，再加入 Kafka/Flink。否则同时调试数据合同、流式状态、云网络、多模态和模型服务，会让任何失败都难以定位。

# 参考资料

[R1] Kaggle, *Home Credit Default Risk — Competition Data*. https://www.kaggle.com/competitions/home-credit-default-risk/data  
[R2] Harnal Ashok, *Home Credit Default Risk Competition — Data Linkages*. https://harnalashok.github.io/credit_risk/data_linkages.html  
[R3] UCI Machine Learning Repository, *Bank Marketing*. https://archive.ics.uci.edu/dataset/222/bank  
[R4] Kaggle, *IEEE-CIS Fraud Detection — Competition Data*. https://www.kaggle.com/competitions/ieee-fraud-detection/data  
[R5] Amazon Science, *Fraud Dataset Benchmark*. https://github.com/amazon-science/fraud-dataset-benchmark  
[R6] Guillaume Jaume et al., *FUNSD: Form Understanding in Noisy Scanned Documents*. https://guillaumejaume.github.io/FUNSD/  
[R7] V. Arlazarov et al., *MIDV-500: A Dataset for Identity Documents Analysis and Recognition on Mobile Devices in Video Stream*. https://arxiv.org/abs/1807.05786  
[R8] DocVQA, *DocVQA Dataset*. https://site.docvqa.org/datasets/docvqa  
[R9] Apache Kafka, *Introduction*. https://kafka.apache.org/documentation/  
[R10] Apache Kafka, *Topics, Partitions, Consumers and Guarantees*. https://kafka.apache.org/0110/documentation.html  
[R11] Apache Kafka, *Design — Message Delivery Semantics*. https://kafka.apache.org/41/design/design/  
[R12] Apache Spark, *Structured Streaming Programming Guide*. https://spark.apache.org/docs/latest/streaming/index.html  
[R13] Apache Flink, *Generating Watermarks*. https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/event-time/generating_watermarks/  
[R14] Apache Flink, *Checkpointing*. https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/  
[R15] Apache Flink, *Fault Tolerance and End-to-End Exactly Once*. https://nightlies.apache.org/flink/flink-docs-master/docs/learn-flink/fault_tolerance/  
[R16] Apache Iceberg, *Evolution*. https://iceberg.apache.org/docs/latest/evolution/  
[R17] Apache Iceberg, *Maintenance*. https://iceberg.apache.org/docs/1.7.2/maintenance/  
[R18] Apache Doris, *Core Capabilities*. https://doris.apache.org/  
[R19] Apache Doris, *Materialized View Overview*. https://doris.apache.org/docs/4.x/query-acceleration/materialized-view/overview/  
[R20] Feast, *Components Overview*. https://docs.feast.dev/getting-started/components/overview  
[R21] Feast, *Online Store*. https://docs.feast.dev/getting-started/components/online-store  
[R22] MLflow, *Experiment Tracking*. https://mlflow.org/docs/latest/ml/tracking  
[R23] MLflow, *Model Registry*. https://mlflow.org/docs/latest/ml/model-registry/  
[R24] Kubernetes, *Deployments and Rolling Updates*. https://kubernetes.io/docs/concepts/workloads/controllers/deployment/  
[R25] Prometheus, *Overview*. https://prometheus.io/docs/introduction/overview/  
[R26] Prometheus, *Grafana Support*. https://prometheus.io/docs/visualization/grafana/  
[R27] OpenTelemetry, *Concepts*. https://opentelemetry.io/docs/concepts/  
[R28] AWS, *MSK Serverless*. https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html  
[R29] AWS, *Running Spark Jobs on EMR Serverless*. https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html  
[R30] AWS, *Amazon Managed Service for Apache Flink*. https://docs.aws.amazon.com/managed-flink/  
[R31] Apache Airflow, *Dags*. https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html  
[R32] Apache Airflow, *Scheduler*. https://airflow.apache.org/docs/apache-airflow/stable/concepts/scheduler.html  

