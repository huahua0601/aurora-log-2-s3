#!/bin/bash

# 更新系统包
echo "正在更新系统包..."
sudo dnf update -y

# 安装 Python3 和 pip3
echo "正在安装 Python3 和 pip3..."
sudo dnf install -y python3 python3-pip

# 验证安装
echo "验证安装..."
python3 --version
pip3 --version

# 安装项目依赖
echo "安装项目依赖..."
pip3 install -r requirements.txt

echo "安装完成！"