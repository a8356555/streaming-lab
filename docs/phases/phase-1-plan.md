# Phase 1 — Walking Skeleton Plan（介面契約 + 任務清單）

> Status: in progress
> 對應 SPEC.md「Phase 1 — Walking Skeleton」與 [db-warehouse-design-discussion §3–4](../../SPEC.md)
> 本文件是 Phase 1 的**唯一契約**：schema、檔案格式、snapshot summary key、query 契約、服務清單、任務清單。
> 實作與測試都以本文件為準；偏離必須先改本文件。

## 0. 命題回顧（Phase 1 要證明的最小判斷）

「Exactly-once 的本質是**進度與資料同一事務**。」本 phase 用最少元件實作一條
`event-gen → Redpanda → (① ClickHouse 秒級 ② Python 落地 job → Iceberg 分鐘級)` 的雙路管線，
把 **Kafka offset + watermark T 原子寫進同一個 Iceberg snapshot**，
並用三個「弱模型無法作弊通過」的 correctness 測試證明它不重、不漏、crash 可恢復。

Phase 1 **不**攻擊它（那是 Phase 2）。Phase 1 只證明正確實作在 happy path + crash 下成立。

## 1. 事件域與 Schema（append-only 電商訂單）

事件只有兩型，append-only（取消 = 新增一列，**不是** UPDATE）：

| event_type | 語意 | 對 GMV 的貢獻 |
|---|---|---|
| `order_created` | 下單 | `+amount` |
| `order_cancelled` | 取消（引用同一 order 的金額） | `−amount` |

**GMV = Σ(created.amount) − Σ(cancelled.amount)**（代數和，縫上可驗證——見 §3.2 解法 C）。

### 1.1 Wire schema（Redpanda / JSONL ground-truth，JSONEachRow 給 ClickHouse）

```jsonc
{
  "event_id":   "uuid4 字串",          // 全域唯一，去重與集合比對的 key
  "order_id":   "uuid4 字串",          // 同一訂單的 created/cancelled 共用；Kafka partition key
  "event_type": "order_created" | "order_cancelled",
  "amount":     "字串形式的 decimal，如 \"123.45\"",  // 錢用字串傳，兩端都 parse 成 Decimal，杜絕 float 漂移
  "user_id":    42,                    // int
  "event_time_ms": 1752230700123        // int，epoch millis，watermark 與縫切分的唯一時間依據
}
```

**設計約束（防漂移）**：
- 金額全程 `Decimal`（Python `decimal.Decimal`、Iceberg `decimal(18,2)`、ClickHouse `Decimal(18,2)`）。
  不重不漏測試要求 sum **全等**，float 會讓「全等」變成「約等」，破壞測試意義。
- 時間只有一個權威欄位 `event_time_ms`（epoch millis, UTC）。不引入 processing-time 欄位參與正確性判定
  （processing time 是 Phase 2 漂移來源之一，Phase 1 不碰）。

### 1.2 產生器的有界亂序保證（watermark 安全性的前提）

Generator 產生 `event_time_ms = start_ms + i * STEP_MS + jitter`，其中 `jitter ∈ [0, LATENESS_WINDOW_MS)`，
以 `i` 遞增順序 produce。故**最大亂序 ≤ LATENESS_WINDOW_MS**。這是 watermark T 安全的前提
（Phase 2 的「遲到事件」場景就是故意打破這個前提，證明縫需要 T 的保守回退）。

Generator 同時把每一筆事件**逐行寫入 `GROUND_TRUTH_PATH`（JSONL）**——這是測試的獨立真值來源，
測試直接讀這個檔算 count/GMV，**絕不透過被測系統（Kafka/CH/Iceberg）算真值**。

## 2. Iceberg 表結構（真理層）

- Catalog：**pyiceberg `SqlCatalog`**，metadata 指標存 SQLite（`CATALOG_URI`），warehouse 在 MinIO（S3 相容）。
  選 SqlCatalog 因為元件最少且 commit 具原子性（SQLite 交易）；REST catalog 是 Phase 2/3 升級項（見 ADR-001）。
- 表：`lake.orders`

```
event_id      string    (required)
order_id      string    (required)
event_type    string    (required)
amount        decimal(18,2) (required)
user_id       long
event_time    timestamptz   (required)   # 由 event_time_ms 轉；Iceberg 內部存 micros
```

- Partition：`day(event_time)`（walking skeleton 測試多在同日，1–2 檔；避免小檔案）。
- 寫入：落地 job 每個 micro-batch 產生一個 Parquet data file，`append` 進表。

### 2.1 Snapshot summary key 命名（offset-in-snapshot 契約——本 repo 的核心）

每次 commit 透過 pyiceberg `append(..., snapshot_properties={...})` 把下列 key 寫進 snapshot summary：

| key | 型別（值皆為字串） | 語意 |
|---|---|---|
| `streaming-lab.kafka.offsets` | JSON obj `{"0": 12345, ...}` | 每 partition 的**下一個要消費的 offset**（= 已消費最大 offset + 1）。恢復時 seek 到此。 |
| `streaming-lab.watermark.event_time_ms` | int 字串 | 本次 commit 後的安全水位 T（epoch millis, UTC）。單調不減。 |
| `streaming-lab.watermark.lateness_window_ms` | int 字串 | 計算 T 用的遲到窗口 W（供審計）。 |
| `streaming-lab.batch.event_count` | int 字串 | 本 commit 寫入事件數（debug/FINDINGS 用）。 |
| `streaming-lab.commit.seq` | int 字串 | 落地 job 的 commit 序號（crash 測試驗證恢復後序號連續遞增）。 |

**原子性論證**：Parquet data file 先寫進 MinIO（此時尚未被任何 snapshot 引用 = 隱形孤兒）；
接著 SqlCatalog 在一個 SQLite 交易內把「新 metadata.json（含新 snapshot + 上述 summary）」設為表的當前指標。
- crash 於 catalog 交易**前**：孤兒 Parquet 不可見 → 不重（restart 從上一 committed offset 重消費、重寫新檔）。
- crash 於交易**後**：offset 已在 snapshot 內 → restart 從已 commit 的 offset 續跑 → 不漏、不重。
→ 進度（offset）與資料（Parquet）**同一事務**提交，這就是 exactly-once 的本質。

### 2.2 watermark T 的計算（§3.1 的實作）

```
T_new = max( T_prev , max_event_time_committed_so_far − LATENESS_WINDOW_MS )
```

- `max_event_time_committed_so_far`：落地 job 至今寫進湖的所有事件的最大 event_time。
- 減去 W 得到安全水位：在「最大亂序 ≤ W」前提下，任何 event_time < T 的事件都已被消費並 commit（標準 watermark 語意）。
- **單調不減**：跨 commit、跨 restart 都不回退（restart 時從上一 snapshot 讀回 T_prev）。
- Phase 1 前提成立（generator 有界亂序）→ 安全性測試綠燈。Phase 2 打破前提 → 展示縫的失效與 T 回退策略（ADR-002）。

## 3. 即時層（ClickHouse Kafka engine + MV）

`docker/clickhouse/init.sql` 建三張表（§2.3 機制：常駐 consumer + 微批觸發 MV）：

```sql
-- 管子（非儲存）：常駐 consumer 從 Redpanda 拉 JSONEachRow
CREATE TABLE lake.orders_queue (
  event_id String, order_id String, event_type String,
  amount Decimal(18,2), user_id Int64, event_time_ms Int64
) ENGINE = Kafka SETTINGS
  kafka_broker_list = 'redpanda:9092',
  kafka_topic_list = 'orders',
  kafka_group_name = 'ch_orders_consumer',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1;

-- 目標儲存表
CREATE TABLE lake.orders_ch (
  event_id String, order_id String, event_type String,
  amount Decimal(18,2), user_id Int64,
  event_time DateTime64(3, 'UTC')
) ENGINE = MergeTree ORDER BY (event_time, event_id);

-- trigger（非排程）：每攢一個 block 觸發一次 SELECT 寫入
CREATE MATERIALIZED VIEW lake.orders_mv TO lake.orders_ch AS
SELECT event_id, order_id, event_type, amount, user_id,
       toDateTime64(event_time_ms / 1000.0, 3, 'UTC') AS event_time
FROM lake.orders_queue;
```

- 即時 GMV：`SELECT sum(if(event_type='order_created', amount, -amount)) FROM lake.orders_ch WHERE event_time >= {T}`。
- 注意：CH 的 Kafka engine 用**自己的 consumer group**（`ch_orders_consumer`），與落地 job 的 offset 管理**完全獨立**——
  兩路各自消費同一條流。這正是「一條流兩個出口」，也是 Phase 2 漂移注入的舞台。

## 4. Query Service（縫 union 查詢）契約

核心函式（library，測試直接呼叫）：

```python
# src/streaming_lab/query/seam.py
@dataclass(frozen=True)
class SeamResult:
    watermark_t_ms: int
    count: int          # lake_count + ch_count（縫合後事件總數）
    gmv: Decimal        # lake_gmv + ch_gmv（代數和）
    lake_count: int
    ch_count: int
    lake_gmv: Decimal
    ch_gmv: Decimal

def seam_query(catalog, ch_client) -> SeamResult: ...
```

**契約（§3.1「同一次查詢用同一個 T」）**：
1. 先讀 `lake.orders` 最新 snapshot 的 `streaming-lab.watermark.event_time_ms` 拿 **T（取一次）**。
2. lake 側：`SELECT` Iceberg `WHERE event_time < T` → count、Σ(±amount)。
3. ch 側：`SELECT ... FROM orders_ch WHERE event_time >= T` → count、Σ(±amount)。
4. 合併：`count = lake_count + ch_count`、`gmv = lake_gmv + ch_gmv`。

**不重不漏的縫語意**：湖對 `event_time < T` 權威、CH 對 `event_time >= T` 權威，兩側以 T 互斥切分 →
每個事件恰好被算一次（湖裡 event_time ≥ T 的列被排除、CH 裡 event_time < T 的列被排除）。

可選 HTTP：`GET /gmv` → `{success, data: SeamResult, error}`（envelope 格式）。Phase 1 非測試路徑，僅 demo 用。

## 5. docker-compose 服務清單（本地，單節點，禁雲端）

專案名 `streaming-lab`，獨立 network，host port 用 19xxx 段避免與同機 s3-lsm（佔 9000/9001）衝突。

| 服務 | image | host port | 用途 |
|---|---|---|---|
| `redpanda` | redpandadata/redpanda | 19092 (kafka) | 單節點 broker，Kafka 接口 |
| `redpanda-console` | redpandadata/console | 18080 | demo 觀察 topic（可選） |
| `clickhouse` | clickhouse/clickhouse-server:24.x | 18123 (http), 19000 (native) | 即時層；init.sql 自動建表 |
| `minio` | minio/minio | 19100 (s3), 19101 (console) | Iceberg warehouse（S3 相容） |
| `createbucket` | minio/mc | — | 一次性建 `lakehouse` bucket |
| `app` | 本 repo Dockerfile（python:3.11-slim） | — | 跑 event-gen / 落地 job / 測試；控制 pyiceberg/pyarrow 版本 |

Python 元件在 `app` 容器內跑（host 是 Python 3.14，pyiceberg/pyarrow 無對應 wheel）。
落地 job 以 `app` 容器內 subprocess 執行 → crash 測試用真 `os.kill(pid, SIGKILL)`。

## 6. 依賴版本鎖定

`requirements.txt`（核心 / 測試路徑）：
```
confluent-kafka==2.5.3       # 落地 job + generator：手動 offset 控制
pyiceberg[s3fs,sql]==0.8.1   # SqlCatalog + S3；append(snapshot_properties=...)
pyarrow==17.0.0
clickhouse-connect==0.8.3
duckdb==1.1.3                 # 對 Iceberg arrow 掃描結果做聚合
```
`requirements-dev.txt`：`pytest==8.3.3`、`pytest-timeout==2.3.1`。
`gateway/requirements.txt`（可選、非測試路徑）：`fastapi`、`uvicorn`、`aiokafka`、`orjson`、`structlog`、`prometheus-client`。

> 版本若在 build 時無對應 wheel，允許在 requirements 內就近調整並在此記錄；不得因此弱化測試。

## 7. 驗收（make demo）

```
make up      # docker compose up -d（redpanda, clickhouse, minio, createbucket）
make init    # 建 Iceberg 表 lake.orders + 確認 CH 表就緒 + 建 topic
make demo    # docker compose run app pytest → 三個 correctness 測試綠燈
make down    # compose down -v
```

三個測試（紅燈骨架先寫，見 §8）：
1. `test_correctness_no_dup_no_loss.py` — 灌 10 萬事件，縫合 count/GMV == JSONL 獨立真值。
2. `test_correctness_crash_restart.py` — 落地 job 真 `kill -9`（commit 前/後隨機），重啟後仍全等、無重複。
3. `test_watermark_safety.py` — `event_time < T` 的事件集合 == 湖裡 `event_time < T` 的集合。

## 8. 紅燈測試的防造假斷言（強制）

| 測試 | 防造假斷言（讓弱模型/退化實作無法蒙混） |
|---|---|
| no_dup_no_loss | 真值只從 `GROUND_TRUTH_PATH` JSONL 直接算；額外斷言 `0 < T`、`lake_count > 0`、`ch_count > 0`（證明縫真的兩側切分，非退化成單邊）；GMV 用 `Decimal` 全等。 |
| crash_restart | 真 `subprocess.Popen` 落地 job + `os.kill(SIGKILL)`；斷言跨重啟 `commit.seq` 連續遞增且恢復未從零重跑；斷言 `count(*) == count(distinct event_id)`（無重複）；縫合 count/GMV == 真值（無漏）。 |
| watermark_safety | 斷言 `T > 0` 且 `event_time < T` 的真值子集非平凡（> 1000 筆，防 T≈0 的空真命題）；斷言集合**相等**（湖有全部 < T 的事件、且湖沒有多出來的）。 |

## 9. 任務清單（≤1hr 粒度）

- [x] T1.1 拆分 repo、隔離 naive_mode、複製 SPEC
- [x] T1.2 本計畫（介面契約）
- [ ] T1.3 `config.py`（endpoints/ports/topic/bucket/catalog uri/W/路徑，env 驅動）
- [ ] T1.4 `events/schema.py`（型別、常數、serialize/deserialize、Decimal 處理）
- [ ] T1.5 `events/generator.py`（有界亂序、寫 JSONL、produce 到 Redpanda）
- [ ] T1.6 `lake/catalog.py`（get_catalog、create_orders_table）
- [ ] T1.7 `lake/watermark.py`（compute_watermark，純函式，可單測）
- [ ] T1.8 `lake/landing_job.py`（手動 offset 消費 → Parquet → append(snapshot_properties) 原子 commit；`__main__` 可跑）
- [ ] T1.9 `realtime/clickhouse.py`（client、wait_for_count、gmv 查詢）+ `docker/clickhouse/init.sql`
- [ ] T1.10 `query/seam.py`（seam_query）+ 可選 `query/service.py`
- [ ] T1.11 docker-compose.yml + Dockerfile + Makefile + requirements
- [ ] T1.12 tests：conftest 固件 + helpers + 三個紅燈測試
- [ ] T1.13 跑 make up/init/demo，迭代到綠燈
- [ ] T1.14 ADR 000–004、FINDINGS 骨架、README、CI workflow

## 10. Phase 1 不做（推 Phase 2/3，記在 docs/roadmap.md）

遲到/重複/亂序/漂移注入、naive mode 對照跑、對帳 job、k6 壓測、hero GIF、CDC upsert、schema registry、多節點/autoscaling。
