# 跨馆 HyperLogLog 摘要报送与合并服务

联盟内各分馆按月、按电子资源上报 **HyperLogLog-128 摘要**（128 个取值
`0～58` 的整数寄存器），服务端逐位置取各馆**现行**摘要的最大值生成合并
摘要，供上级数仓跨地区估算月度去重读者数。无任何报送时合并寄存器全为 0。

## 目录结构

```
app/
  hll.py       # 统一哈希口径（SHA-256 → 128 寄存器，rank 1～58）与入参校验
  db.py        # SQLite 存档、增量合并、换报/撤回的事务处理
  web.py       # Flask 路由与报送页
  templates/   # 报送/撤回/下载页面
tests/         # pytest 用例（含并发与写入失败回滚）
examples/      # 仅标准库的报送示例客户端
Dockerfile / docker-compose.yml / wsgi.py
```

## 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

SQLite 数据持久化在命名卷 `hll-data`（容器内 `/data/hll.sqlite3`）。

### 本地直跑

```bash
pip install -r requirements.txt
export HLL_DB_PATH=./data/hll.sqlite3
python wsgi.py          # Flask 开发服务器
# 生产 WSGI（与容器内一致）：
gunicorn --bind 0.0.0.0:8000 --workers 2 --threads 4 wsgi:app
```

## 统一哈希口径（各馆必须一致）

对读者唯一标识做 **SHA-256**，取前 8 字节（64 bit）：

- 低 7 bit → 寄存器位置（0～127，共 128 个）；
- 其余 57 bit 从最低位起统计前导 0 的个数加 1 → rank（范围 1～58），
  寄存器保存该位置见过的最大 rank，无贡献为 0。

参考实现见 `app/hll.py` 的 `hll_register()` / `build_summary()`。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/reports` | 报送/换报，JSON 体或 multipart 表单上传 JSON 文件 |
| `POST`/`DELETE` | `/reports/withdraw` | 撤回本馆报送 |
| `GET` | `/reports/<month>/<resource>/<branch>` | 查本馆现行报送 |
| `GET` | `/merged/<month>/<resource>/download` | 下载已保存的合并摘要 JSON |
| `GET` | `/health` | 健康检查 |

报送 JSON 格式：

```json
{
  "month": "2026-11",
  "resource": "ER-0001",
  "branch": "LIB-A",
  "registers": [0, 0, 0, "/* 共 128 个，取值 0～58 */"]
}
```

curl 示例：

```bash
# 报送（同馆同键再传即换报，整行覆盖）
curl -X POST localhost:8000/reports \
  -H 'Content-Type: application/json' \
  -d @report.json

# 撤回（POST JSON，或 DELETE 带查询串均可）
curl -X POST localhost:8000/reports/withdraw \
  -H 'Content-Type: application/json' \
  -d '{"month":"2026-11","resource":"ER-0001","branch":"LIB-A"}'
curl -X DELETE "localhost:8000/reports/withdraw?month=2026-11&resource=ER-0001&branch=LIB-A"

# 下载合并摘要
curl -OJ localhost:8000/merged/2026-11/ER-0001/download
```

浏览器访问 `/` 可用表单完成报送、撤回、下载；程序化调用（带
`Accept: application/json`）出错时返回 JSON 错误体（400/404）。

## 合并算法与关键约束的实现

### 1. 逐位置取最大值，换报/撤回不遍历其他馆摘要

除馆报表 `branch_reports` 和合并表 `merged_summaries` 外，另维护
`register_counts(month, resource, position, value, cnt)`：按每个寄存器
位置保存完整的**取值直方图**（该取值当前有多少个馆贡献）。

- 报送/换报：只读取**本馆**旧寄存器，计算每个位置的差分（旧值 `-1`、
  新值 `+1`），按主键 upsert/delete 直方图行；
- 撤回：只对本馆旧值做 `-1` 差分；计数归零则删除该直方图行；
- 合并摘要：一条 `GROUP BY position` + `MAX(value)` 聚合得到逐位置
  最大值（无计数的位置即 0），全程不扫描其他馆的 `branch_reports` 行；
- 最大值馆撤回或换报降值时，次大值可直接从直方图恢复，不会丢失；
  同值多馆用 `cnt` 记录，撤回其中一个馆不会错误降低最大值；
- 直方图规模只与寄存器数（128）和取值域（1～58）有关，不随馆数膨胀。

### 2. 馆报与汇总一并生效，写入失败沿用原数据

每次报送/换报/撤回都在**单个 `BEGIN IMMEDIATE` 事务**内完成三件事：
写馆报、更新 `register_counts`、重写 `merged_summaries`。任何一步失败
或 commit 失败即 `ROLLBACK`，库内保留原数据（测试
`test_commit_failure_keeps_original_data` 模拟 commit 失败验证）。

### 3. 不同馆并发更改，每笔成功操作都计入汇总

- WAL 模式 + `busy_timeout=30s` + 应用层指数退避重试，写事务经
  `BEGIN IMMEDIATE` 在 SQLite 层串行化，杜绝写写冲突导致的丢更新；
- 每个事务只做本馆差分，事务结束后的合并摘要始终等于全部已提交馆报的
  逐位置最大值（并发用例见 `test_concurrent_*`）。

### 4. 无报送与全撤回

- 从未报送过的 `(月份, 资源号)`：下载返回 404；
- 报送后又全部撤回：合并摘要仍已保存，寄存器为 128 个 0。

## 测试

```bash
python -m pytest -q
```
