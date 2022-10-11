FROM python:3.9.9-bullseye as builder
RUN apt-get update && apt-get install python-dev libsasl2-dev gcc -y
COPY . .
RUN /bin/bash -c "python -m venv target && . ./target/bin/activate && pip install --upgrade pip setuptools wheel && pip install ."

FROM python:3.9.9-bullseye
RUN apt-get update && apt-get install python-dev libsasl2-dev gcc -y
COPY --from=builder /target /target
