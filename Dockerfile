FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jre-headless \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && java -version \
    && python -c "import pyspark; assert pyspark.__version__ == '3.5.3'; print(pyspark.__version__)"

COPY . .

CMD ["python", "scripts/run_resume_demo.py"]
