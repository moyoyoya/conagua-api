FROM python:3.11.1

WORKDIR /project

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get -y install cron
add ./project .


CMD [ "python", "project/main.py" ]