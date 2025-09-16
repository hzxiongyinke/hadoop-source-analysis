#!/bin/bash

# Hadoop源码深度解析 - Docsify启动脚本

echo "🚀 启动Hadoop源码深度解析文档站点..."
echo "================================================"

# 检查Python是否安装
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
else
    echo "❌ 错误: 未找到Python，请先安装Python"
    exit 1
fi

# 检查端口是否被占用
PORT=8080
while lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>/dev/null ; do
    echo "⚠️  端口 $PORT 已被占用，尝试端口 $((PORT+1))"
    PORT=$((PORT+1))
done

echo "📦 使用Python内置服务器启动文档站点..."
echo "================================================"
echo "📖 访问地址: http://localhost:$PORT"
echo "🛑 停止服务: Ctrl+C"
echo "================================================"

# 启动Python HTTP服务器
$PYTHON_CMD -m http.server $PORT
