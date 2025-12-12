# 统一 Dockerfile - 前后端合并
# 前端构建阶段
FROM node:18-alpine AS frontend-builder
WORKDIR /frontend
COPY binance-index-frontend/package*.json ./
RUN npm install
COPY binance-index-frontend/ ./
RUN npm run build

# 后端构建阶段
FROM maven:3.9-eclipse-temurin-17 AS backend-builder
WORKDIR /backend
COPY binance-market-index/pom.xml ./
RUN mvn dependency:go-offline
COPY binance-market-index/src ./src
RUN mvn package -DskipTests

# 最终运行镜像
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# 安装 nginx
RUN apk add --no-cache nginx

# 复制后端 jar
COPY --from=backend-builder /backend/target/*.jar app.jar

# 复制前端静态文件
COPY --from=frontend-builder /frontend/dist /usr/share/nginx/html

# 复制 nginx 配置
COPY nginx.conf /etc/nginx/http.d/default.conf

# 复制启动脚本
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# 创建数据目录
RUN mkdir -p /app/data
VOLUME /app/data

# 暴露端口
EXPOSE 80 8080

# 启动
CMD ["/app/start.sh"]
