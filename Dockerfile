FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \n        curl build-essential pkg-config \n    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \n       | sh -s -- -y --default-toolchain stable \n    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
