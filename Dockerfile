# Do not modify nor delete this file unless you know what you are doing.
# If you break this file your project is not going to work anymore on our platform.
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
