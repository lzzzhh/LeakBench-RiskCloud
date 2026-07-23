# LeakBench-RiskCloud

云原生大数据多模态风控与数据泄漏治理平台。

## 目录结构

```
riskcloud/           # 平台核心代码
  contracts/         # 统一数据合同（Event, PredictionPoint, FeatureCatalog, DocumentParseResult）
  adapters/          # 数据集适配器接口
tests/               # 测试
docs/                # 设计文档
```

## 阶段

- [x] Phase 0: 合同与接口定义
- [ ] Phase 1: Home Credit Spark 垂直链路
- [ ] Phase 2: Feature Store 与本地训练
- [ ] Phase 3: Kafka/Flink 实时链路
- [ ] Phase 4: LeakBench Promotion Gate
- [ ] Phase 5: 多模态文档支线
- [ ] Phase 6: React 看板
- [ ] Phase 7: 云端部署与压测
