FROM python:3.12-slim
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" py-ip2region
WORKDIR /app
COPY app.py /app/app.py
EXPOSE 8088
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8088"]
