# 时间序列存储库

纯 Go、嵌入式本地存储，不依赖独立数据库服务。

## 能力

- `Append`：按 `series + timestamp` 追加观测，重复键返回 `ErrObservationExists`。
- `Revise`：迟到修订只替换值，不改变时间戳和范围归属。
- `Snapshot`：捕获明确数据版本；在 `Close` 前，压缩和过期删除不会改变其读取结果。
- `Range`：读取 `[start, end)` 半开区间，输出按时间戳排序，乱序写入不影响边界。
- `DeleteBefore`：写入版本化墓碑，旧快照仍可读，新快照不可见。
- `Compact`：把 WAL 和历史段合并为不可变段；旧物理文件等待在途快照释放后删除。
- 重启恢复：已 `Sync` 确认的 WAL、分段和原子清单可恢复；压缩中断后重复版本会去重并完成清理。
- `RegisterAggregator` / `Downsample`：按固定时间桶生成降采样版本。迟到 `Revise` 只更新受影响桶的新版本，旧快照仍读到原来的原始值与聚合值；空桶也显式产出，重跑与恢复按来源版本跳过，不会重复聚合。
- `DeleteBefore` 对降采样结果做级联：整桶过期写聚合墓碑，跨 cutoff 的桶用仍存活的点重算；证据只做版本化删除，物理清理仍由 `Compact` 等待在途快照后完成。

## 示例

```go
store, err := timeseries.Open("./data", nil)
if err != nil { panic(err) }
defer store.Close()

version, err := store.Append(timeseries.Observation{
    Series:    "cpu",
    Timestamp: 1700000000,
    Data:      []byte("0.42"),
})

snapshot := store.Snapshot()
defer snapshot.Close()
rows := snapshot.Range("cpu", 1700000000, 1700003600)
fmt.Println(version, rows)

store.RegisterAggregator("sum", 60, func(points []Observation) []byte { ... })
store.Downsample("sum", "cpu", 1700000000, 1700003600)
buckets := snapshot.Aggregates("sum", "cpu", 1700000000, 1700003600)
```

## 验证

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```
