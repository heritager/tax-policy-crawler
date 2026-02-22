# tax-policy-crawler

从国家税务总局等站点采集税收政策与政策解读数据，默认输出到 Excel（可选写入 Elasticsearch）。

## 当前状态（2026-02-22）

项目已完成一次现代化改造：

- 适配新版本 Scrapy（移除 `scrapy.contrib` 依赖）。
- 适配新版本 Elasticsearch Python 客户端（8/9）。
- 默认不依赖内网 ES、代理池、Remote Chrome 服务。
- `chinatax` 两个主爬虫改为调用官方 `search5` JSON 接口，稳定性高于旧 DOM 解析。

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 运行（默认抓政策法规 + 政策解读）

```bash
python entrypoint.py
```

运行完成后会在项目根目录生成 `tax_policy.xls`。

常用参数：

```bash
python entrypoint.py --max-pages 1 --page-size 20 --export-excel 1 --use-elasticsearch 0
```

- `--spiders` 可选：`TaxPolicyCrawler`、`TaxPolicyExplainCrawler`
- `--max-pages` / `--page-size` 覆盖配置项
- `--export-excel` 与 `--use-elasticsearch` 支持 `0/1`

## 环境变量

### 基础运行

- `DOWNLOAD_DELAY` 默认 `1`
- `CONCURRENT_REQUESTS_PER_IP` 默认 `8`
- `ROBOTSTXT_OBEY` 默认 `false`
- `EXPORT_EXCEL` 默认 `true`

### 抓取范围

- `CHINATAX_PAGE_SIZE` 默认 `20`
- `CHINATAX_MAX_PAGES` 默认 `0`（不限制页数）
- `CHINATAX_POLICY_LABELS` 默认  
  `法律,行政法规,国务院文件,税务部门规章,税务规范性文件,财税文件,其他文件,工作通知`

### 代理（可选）

- `USE_PROXY` 默认 `false`
- `PROXY_HOST` 默认 `127.0.0.1`
- `PROXY_PORT` 默认 `5000`

### Elasticsearch（可选）

- `USE_ELASTICSEARCH` 默认 `false`
- `ES_URL` 优先（例如 `http://127.0.0.1:9200`）
- 若未设置 `ES_URL`，则使用：
  - `ES_SCHEME` 默认 `http`
  - `ES_HOST` 默认 `127.0.0.1`
  - `ES_PORT` 默认 `9200`

## 说明

- 旧的 `Robot` 系列脚本和 `shui5` 站点抓取仍保留，但可能受目标站 WAF/验证码影响。
- 若需大规模稳定采集，建议增加断点续跑、接口响应落盘和定时监控。
