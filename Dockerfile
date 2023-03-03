FROM python:3.11.1

WORKDIR /project

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

add ./project .


CMD [ "python", "project/main.py" ]