# Sử dụng Python base image
FROM python:3.9

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Copy toàn bộ mã nguồn vào container
COPY . .

# Cài đặt thư viện Python từ requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Load biến môi trường từ .env (được Docker Compose cung cấp)
ENV DATABASE_URL=${DATABASE_URL}

# Chạy ứng dụng
CMD ["python", "main.py"]