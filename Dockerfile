FROM python:3.9

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENV DATABASE_URL=${DATABASE_URL}

CMD ["python", "main.py"]