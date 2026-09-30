# CEMS 数据管道四个 Python 服务的统一镜像（设备层/网关层/接入层/展示层共用）
# 各服务只是启动命令不同，见 docker-compose.yml 里的 command
FROM python:3.12-slim

# - TZ：容器时区，必须与 TDengine 容器的时区一致，否则入库时间戳会整体偏移 8 小时
#       （python:3.12-slim 自带 /usr/share/zoneinfo，不需要额外装 tzdata）
# - PYTHONUNBUFFERED：日志实时输出，docker logs 才能立刻看到
ENV TZ=Asia/Shanghai \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 同时把系统时区链接过去，兼容不读 TZ 环境变量的工具
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# 先装依赖再拷代码：改代码时不会重复下载依赖（利用构建缓存）
COPY requirements.txt ./
# PIP_INDEX：默认用阿里云镜像；
#   ⚠️ 2026-10-01 实测：清华源（原默认）对 pip 返回 403 Forbidden，容器内与宿主机都一样，
#      中科大 / 华为云同样不通，只有阿里云可用 → 故默认改成阿里云。
#   想换源/回官方：docker build --build-arg PIP_INDEX=https://pypi.org/simple .
ARG PIP_INDEX=https://mirrors.aliyun.com/pypi/simple/
RUN pip install --no-cache-dir --retries 10 --timeout 60 \
        -i ${PIP_INDEX} \
        -r requirements.txt

COPY src/ ./src/

# 默认跑设备层，其它服务在 compose 里用 command 覆盖
CMD ["python", "src/device/modbus_server.py"]
