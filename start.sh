#!/bin/sh

# 启动后端 (后台运行) - JVM堆内存6G，使用G1GC优化内存回收
echo "Starting backend with 6G heap (G1GC)..."
java -Xms1g -Xmx6g \
     -XX:+UseG1GC \
     -XX:MaxGCPauseMillis=200 \
     -XX:+ExplicitGCInvokesConcurrent \
     -XX:+HeapDumpOnOutOfMemoryError \
     -jar /app/app.jar &

# 等待后端启动
sleep 5

# 启动 nginx (前台运行)
echo "Starting nginx..."
nginx -g "daemon off;"
