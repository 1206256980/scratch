# 币安 U本位市场指数

实时监控币安U本位合约整体市场走势，通过计算所有山寨币（不含BTC、ETH）的交易量加权平均涨跌幅，生成一个综合市场指数。

## 功能特性

- 📊 实时市场指数折线图
- ⏰ 每5分钟自动采集数据
- 📅 支持查看6小时/12小时/1天/3天历史走势
- 🚀 启动时自动回补3天历史数据
- 🎨 现代暗色主题界面

## 快速开始

### Docker 部署（推荐）

```bash
# 拉取镜像
docker pull ghcr.io/你的用户名/仓库名:latest

# 运行
docker run -d \
  --name binance-index \
  -p 80:80 \
  -v binance-index-data:/app/data \
  ghcr.io/你的用户名/仓库名:latest
```

访问 `http://localhost` 即可。

### 本地开发

**后端：**
```bash
cd binance-market-index
mvn spring-boot:run
```

**前端：**
```bash
cd binance-index-frontend
npm install
npm run dev
```

## 项目结构

```
├── binance-market-index/     # 后端 (Spring Boot)
│   ├── src/main/java/
│   └── pom.xml
├── binance-index-frontend/   # 前端 (Vite + React)
│   ├── src/
│   └── package.json
├── Dockerfile                # 统一镜像构建
├── nginx.conf                # Nginx配置
├── start.sh                  # 容器启动脚本
└── .github/workflows/        # GitHub Actions
```

## 配置说明

主要配置在 `binance-market-index/src/main/resources/application.properties`：

```properties
# 数据采集间隔（分钟）
index.collect.interval-minutes=5

# 历史回补天数
index.backfill.days=3

# 排除的币种
index.exclude-symbols=BTCUSDT,ETHUSDT
```

## 数据持久化

使用 Docker Volume 挂载 `/app/data` 目录可持久化数据：

```bash
docker run -v binance-index-data:/app/data ...
```

即使不挂载，每次启动也会自动回补3天历史数据。

## API 接口

| 接口 | 说明 |
|------|------|
| `GET /api/index/current` | 获取当前指数 |
| `GET /api/index/history?hours=72` | 获取历史数据 |
| `GET /api/index/stats` | 获取统计信息 |

## License

MIT
