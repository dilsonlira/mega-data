FROM python:3.6-slim
WORKDIR /src
COPY requirements.txt .
COPY ./src .
RUN python -m pip install --no-cache-dir --upgrade pip \
    pip install -r requirements.txt
CMD sh -c \
    flake8 --exit-zero && \
    mypy . && \
    sleep 10 && \
    python mega-data.py
