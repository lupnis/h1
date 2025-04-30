FROM python:3.10-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

COPY ./core /app/core

COPY ./utils /app/utils

COPY ./configs.json /app/configs.json

COPY ./main.py /app/main.py

CMD ["uvicorn", "--host=0.0.0.0", "--port=16384", "main:app", "--workers=4"]
