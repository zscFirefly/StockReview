# StockReview

每日A股复盘数据汇总到飞书表格

## get_data_and_update_daily脚本说明

### 功能说明

该脚本用于抓取每日A股涨停板数据，并将数据同步到飞书表格和本地Excel文件。

#### 数据来源

- API：https://apphis.longhuvip.com/w1/api/index.php

#### 数据输出

##### 1. 飞书表格
数据同步到以下三个飞书表格：

- **大盘统计** (sheet_id: `b93e41`)
  - 列：日期、上涨家数、下跌家数、涨停、跌停、炸板率、昨日涨停表现

- **股票维度** (sheet_id: `lWz5Xh`)
  - 列：日期、板块代码、板块名称、股票代码、股票简称、涨停原因、概念标签、流通市值、总市值、市盈率、涨停说明、连板天数

- **板块维度** (sheet_id: `P5PpQI`)
  - 列：日期、板块代码、板块名称、涨停数量

##### 2. 本地Excel文件
- 文件名：`板块涨停_关系型_全量_1230.xlsx`
- 包含三个sheet：板块维度、股票维度、大盘统计

### 使用方法

#### 运行脚本

```bash
python get_data_and_update_daily.py
```

#### 默认配置

- 默认抓取当天数据（可通过修改 `start` 和 `end` 变量来指定日期范围）
- 每次请求间隔 0.5 秒

#### 自定义日期范围

在脚本主循环部分修改日期范围：

```python
# 默认跑今天
start = date.today()
end   = date.today()

# 或指定具体日期
# start = date(2025, 12, 31)
# end   = date(2025, 12, 31)
```

### 函数说明

| 函数名 | 功能 |
|--------|------|
| `get_ztdt_data(day)` | 获取指定日期的涨停板数据 |
| `parse_to_df(json_data, day)` | 解析JSON数据，转换为三个DataFrame（板块、股票、大盘统计） |
| `append_to_excel()` | 追加数据到本地Excel文件 |
| `append_to_feishu()` | 写入大盘统计数据到飞书 |
| `append_stock_to_feishu()` | 写入股票数据到飞书 |
| `append_plate_to_feishu()` | 写入板块数据到飞书 |

### 注意事项

1. 需要配置有效的飞书 access_token
2. 确保飞书表格已创建且具有写入权限
3. 脚本会自动去重，避免重复写入相同日期的数据


## get_data_local脚本说明
用于获取全量的历史数据，仅保存到本地Excel文件