FROM python:3

RUN mkdir -p /opt/app && \
    useradd app && \
    chown -R app:app /opt/app

COPY requirements.txt /opt/app/requirements.txt

RUN pip install -r /opt/app/requirements.txt