FROM python:3.10-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY ./base /app/base

COPY ./core /app/core

COPY ./tasks /app/tasks

COPY ./utils /app/utils

COPY ./configs.json /app/configs.json

COPY ./main.py /app/main.py

CMD ["python", "main.py", "--workers=2"]
