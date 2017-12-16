#FROM bluelens/python:3.6
FROM bluelens/ubuntu-16.04:py3
MAINTAINER bluehackmaster <master@bluehack.net>

RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

CMD ["python3", "main.py"]
