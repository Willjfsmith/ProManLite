FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8000

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# ANTHROPIC_API_KEY must be supplied at runtime, e.g.
#   docker run -e ANTHROPIC_API_KEY=sk-ant-... -p 8000:8000 team-skills
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
