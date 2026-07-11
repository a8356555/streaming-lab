# Spec: streaming-lab — Distributed Correctness Lab

> Status: not started（素材已存在）
> 預算：Phase 1 = 1–2 週末、Phase 2 = 1–2 週末、Phase 3 = 1 週末、Phase 4 = 2–3 週末
> 素材來源：`system-design/data-intensive-app` + `data-intensive-app-flink`（拆出成獨立 repo）
> 理論依據：[db-warehouse-design-discussion-2026-07.md](../db-warehouse-design-discussion-2026-07.md) §3–4

## 命題（README 第一屏）

> 「Exactly-once 不是一個開關，是『進度與資料同一事務』。本 repo 實作一個 Kafka → ClickHouse + Iceberg 的雙路架構，然後**系統性地攻擊它**：遲到、重複、亂序、crash——並展示天真實作（dual-write、外部 offset）在哪些時序下產生錯誤數字。」

要證明的判斷力：
- Dual-write 是 bug 不是架構（現有 `hybrid_source_of_truth.py` 依序寫 Kafka/冷儲存/CH 無原子協調——**保留為 naive mode 當反面教材**）
- Offset 與資料原子提交 = exactly-once 的本質
- 縫的正確性：watermark T、union 查詢的不重不漏
- Lambda 漂移的發生與偵測（對帳）

## 現有素材盤點（agent 執行前先讀）

已有：FastAPI gateway + async Kafka producer、PyFlink jobs（event-time window + WatermarkStrategy）、Redis/CH sink、multi-tier storage 概念、Prometheus metrics、docker-compose。
缺（= 本 spec 的工作）：Iceberg 層、原子 offset commit、縫 union 查詢、correctness/chaos 測試、對帳 job、壓測數字、去行銷化的 README。

## Repo 建立方式（拆分機制）

1. 建新 repo `streaming-lab`（命名備選：`exactly-once-lab`、`streaming-correctness-lab`——ADR-000 定案）
2. 從 `system-design` **複製** `data-intensive-app/` 與 `data-intensive-app-flink/` 進來當起點（fresh history 即可，不需 git subtree；舊 repo 是素材不是祖先）
3. 保留可用件：FastAPI gateway、async Kafka producer、PyFlink jobs（參考用）、docker-compose、Prometheus 設定
4. `hybrid_source_of_truth.py` 改名收進 `naive_mode/`——它的 dual-write 邏輯是 Phase 2 的反面教材，不修它，攻擊它
5. 舊 `system-design` repo：streaming-lab Phase 1 完成後，README 加「data-intensive 部分已演化為 → streaming-lab」。**保持 public、不 archive**——2023 年的 commit 歷史是演化敘事的可驗證證據（對抗「AI 生成」懷疑的鐵證），streaming-lab README 的敘事段落要回連它
6. **敘事定調（README 與面試共用）**：「我 2023 年寫了這個系統，當時認為 dual-write 是合理架構；2026 年我理解了它為什麼錯，寫測試證明它在哪些時序下產生錯誤數字，然後重建了正確版」——自我審查弧線是本 repo 相對全新專案的核心加分

## 技術決策（預定）

- Broker：**Redpanda**（單 binary，docker-compose 輕）；接口即 Kafka
- 湖：**MinIO + Iceberg（pyiceberg）**——不用 Spark，落地 job 用純 Python consumer 批次寫 Parquet + pyiceberg commit（元件最少化；Flink 版是 backlog）
- 即時層：ClickHouse（單節點）
- 事件域：沿用電商訂單（`order_created`/`order_cancelled`，**append-only**——mutable CDC 是 backlog 深水區）
- 指標：GMV = created − cancelled 的代數和（縫上可驗證）

## Phase 1 — Walking Skeleton：正確的最小實作（1–2 週末）

```
event-gen → Redpanda → ① CH（Kafka engine + MV，秒級）
                      → ② Python 落地 job → Parquet → Iceberg commit（分鐘級）
                            └─ Kafka offsets + watermark T 寫進 snapshot summary（原子）
query svc: 讀最新 snapshot 拿 T → Iceberg(<T) ∪ CH(≥T) → 合併
```

必做測試（correctness suite v1）：
1. **不重不漏基準**：灌 10 萬事件，縫合查詢的 count/sum 與 ground truth 全等
2. **Crash-restart**：落地 job 在 commit 前/後隨機 kill -9，重啟後重跑——結果仍全等（offset 從 snapshot 恢復）
3. T 的安全性：驗證「event_time < T 的資料保證已在湖裡」

驗收：docker-compose up → `make demo` 跑完三個測試綠燈。

## Phase 2 — 攻擊它（1–2 週末，本 repo 的價值核心）

**Chaos suite**（每個場景 = 一個 pytest，注入器可重用——它同時是 data-agent 未來的 eval ground truth）：
1. 遲到事件（event_time 舊、到達晚）：驗證 T 回退邏輯，縫上不漏
2. 重複事件（producer retry 模擬）：驗證去重策略，不重
3. 亂序（partition 間 skew）
4. Commit 中途 kill（Phase 1 已有，擴成多時點注入）
5. **漂移注入**：故意只改 CH 端 MV 的邏輯（模擬 bugfix 只修一邊）

**Naive mode 對照組**（一個 flag 切換）：
- dual-write（沿用舊 `hybrid_source_of_truth` 邏輯）+ offset 存 consumer group
- 同一套 chaos suite 跑 naive mode → **展示它壞掉**：哪個場景、差多少、什麼時序
- FINDINGS 的殺手表格：「場景 × naive/correct × 錯誤率」

**對帳 job**：每日窗（模擬 D-2）比對 CH vs Iceberg 核心指標，>0.1% 告警；配合漂移注入展示「注入當天曲線跳起來」。

## Phase 3 — 壓測與 FINDINGS（1 週末）

- k6 壓測 gateway（ingest path）與 query svc（union 查詢）：併發 vs p50/p99 曲線、找出瓶頸、修一輪、前後對照圖
- FINDINGS.md：chaos 對照表 + 壓測前後圖 + 「哪個結果出乎意料」
- README 重寫：去行銷化（刪掉『10x better』），第一屏 = 命題 + hero artifact

**Hero artifact**（README 第一屏，本 repo 最重要的交付之一）：一個 GIF/動圖——左右分屏，同一批事件灌入，左邊 naive mode 的 GMV 數字在 chaos 注入後漸漸偏離 ground truth、右邊 correct mode 的對帳曲線保持平穩。10 秒講完整個 repo。（實作：terminal 錄影 asciinema→gif，或 matplotlib 動畫。）

**生產系統連結（範例）**：
- offset-in-snapshot → 「Flink Iceberg sink / Kafka Connect Iceberg sink 內建的就是這個機制，我從零實作了它」
- naive dual-write 的錯誤率 → 「這就是為什麼 Uber/Netflix 的 ingest 管線都繞道 transactional commit，而不是『寫兩邊』」
- 對帳 job → 「Netflix 的 data auditor、Airbnb 的 data quality check 是同一 pattern 的生產版」

**Writeup 題目（本 repo 的文章是全計畫最強的一篇）**：「我證明了自己 2023 年的架構是錯的」——自我審查敘事，投 HN / r/dataengineering。

## Phase 4 — Scale-to-Failure（2–3 週末）

> 目標敘事：「Phase 1–3 證明它是對的；Phase 4 量測**正確性在多大流量下變得多貴**，以及 100x 之後什麼會壞。」對應職涯敘事：交易所（Million/day、正確性極嚴）→ ad-tech 級（Billion/day、正確性換吞吐）。
> 背景數學：1B/day = 平均 ~11.6K events/sec、尖峰 50–100K/sec——單機可達，所以本 phase 的價值不在數字，在**撞牆與診斷的過程記錄**。

### 4.1 Load-to-failure 曲線（主軸，先做）

- 固定硬體（記錄規格），事件產生速率 1K → 5K → 10K → 25K → 50K → 100K events/sec 階梯上推
- 每個檔位記錄：端到端延遲 p50/p99/p999、落地 lag、CH ingest lag、資源水位（CPU/mem/disk/net）
- **每撞一堵牆 = 一則 FINDINGS entry**：現象 → 診斷過程（用了什麼工具、看了什麼指標）→ 根因 → 修法 → 修後曲線。預期的牆（僅供對照，不得預設答案）：落地 job commit 頻率、CH part merge 壓力、producer 批次設定、Python GIL、page cache 耗盡
- 驗收：一張「吞吐 vs p99 延遲」總圖，標注每堵牆的位置與修復點（本 phase 的 hero artifact，README 第一屏第二張圖）
- **防造假**：吞吐由獨立的下游計數驗證（CH 端實際落地行數/秒），不得用 producer 自報速率；每檔位持續 ≥10 分鐘（排除 burst 假象）；所有 run 的原始指標 CSV 入庫

### 4.2 Hot key / partition skew

- 事件 key 從均勻分布切換成 Zipfian（s=1.2，模擬 user_id/campaign_id skew）
- 展示：單一 hot partition 使整體吞吐塌陷的曲線 → 實作緩解（key salting 或 producer 端預聚合，二選一，ADR 記錄）→ 前後對照
- 驗收：skew 注入前/後/緩解後三條吞吐曲線；per-partition lag 分布圖
- **防造假**：Zipfian 產生器有單元測試驗證分布；緩解後必須驗證正確性測試（Phase 1 三件套）仍全綠——**吞吐修復不得以正確性為代價，若有代價必須量出來**

### 4.3 Backpressure 級聯

- 用 docker 資源限制把 ClickHouse 人為變慢（CPU throttle），觀察 gateway 行為：優雅降級還是 OOM/崩潰？
- 實作：bounded queue + load shedding（明確的丟棄策略：丟什麼、保什麼、如何告知上游）
- 驗收：CH 恢復後系統自動回穩、無資料遺失（透過 Kafka 重播補齊）的證明測試
- **防造假**：shedding 期間的事件去向必須可審計（丟棄計數器 + 重播後對帳全等）

### 4.4 Exactly-once 的成本曲線（與 Phase 1 直接對接，最重要的單一發現）

- 同一硬體、同一事件流，兩種落地模式對照：①Phase 1 的 transactional commit（offset-in-snapshot）②at-least-once + 冪等消費（下游去重）
- 量測兩種模式的吞吐上限與延遲曲線，**找出交叉點**
- 驗收：兩條曲線一張圖 + 一段（Alan 手寫）解讀：「X K/sec 以下 exactly-once 的代價可忽略；超過 Y 之後 at-least-once + idempotency 是唯一選擇——這就是 Billion 級公司的設計為什麼長那樣」
- **防造假**：②的冪等性必須真的實作並通過重複注入測試，不得拿「裸 at-least-once」墊高對照組差距

### 4.5 精確 → 近似的轉折

- 高基數 distinct count（daily unique users）：精確計數 vs HyperLogLog，量測記憶體 × 誤差 × 吞吐三方取捨
- 驗收：HLL 誤差實測值 vs 理論界；記憶體對照表
- **防造假**：誤差對照的 ground truth 用離線精確計算，資料集基數 ≥10M

### 4.6 容量規劃 writeup（收尾，Alan 主筆）

- 「把本 lab 擴到 1B/day：紙上工程」——partition 數、broker/節點數、儲存與保留期成本、跨 AZ 費、哪些單機設計必須換掉、SLO 怎麼定
- 每個估算都引用 4.1–4.5 的實測數字當基礎（「我的 back-of-envelope 有實測背書」）
- 這篇同時是 system design 面試的預演稿與本 phase 的 writeup

### Phase 4 生產系統連結（範例）

- hot key 緩解 → 「Kafka 的 sticky partitioner 與 Flink 的 local aggregation 就是這兩個選項的生產版」
- exactly-once 交叉點 → 「這解釋了為什麼金融結算系統選 transactional 而 ad impression 管線選 at-least-once + dedup」
- HLL → 「ClickHouse 的 uniq() 預設就是近似演算法——我量出了它換到的東西」

## 必交付 Artifacts

- SPEC.md、FINDINGS.md、CI（correctness suite 進 CI；chaos 可 nightly）
- ADR 至少：①offset-in-snapshot vs 外部 checkpoint ②T 的回退策略（遲到窗口）③為什麼 append-only（mutable 縫的困難）④Python consumer vs Flink
- Non-goals：無 CDC upsert（backlog）、無 autoscaling、無多 region、無 schema registry

## 面試防守題

- exactly-once 在什麼邊界失效？（sink 不冪等/不事務時；跨系統邊界）
- 為什麼 offset 不能存 consumer group？crash 時序畫出來
- 遲到資料怎麼處理？T 為什麼要保守回退？
- dual-write 為什麼是 bug？什麼情況下它「看起來」能用？
- 你怎麼證明它是對的？（→ 指著 chaos suite 回答）

Phase 4 追加：
- 你的系統第一堵牆撞在哪？怎麼診斷的？（→ load-to-failure FINDINGS）
- exactly-once 和 at-least-once 你什麼時候選哪個？（→ 指著交叉點圖回答，並連到交易所 vs ad-tech 的域差異）
- hot key 怎麼發現、怎麼解？緩解的正確性代價是什麼？
- 1B/day 需要什麼？（→ 容量規劃 writeup，每個數字有實測背書）
- 你的單機量測外推到分散式，哪些結論會失效？（誠實答：跨節點協調成本、rebalance、網路——這是紙上規劃的邊界）
