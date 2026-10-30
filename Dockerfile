FROM python:3.14.0-slim-trixie@sha256:0aecac02dc3d4c5dbb024b753af084cafe41f5416e02193f1ce345d671ec966e as python-base
RUN rm -f /etc/apt/apt.conf.d/docker-clean \
    && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' | tee /etc/apt/apt.conf.d/keep-cache
ARG PYTHON_REQUIREMENTS_FILE=requirements.txt
ADD "$PYTHON_REQUIREMENTS_FILE" /tmp/requirements.txt

RUN apt-get -qq update \
    && apt-get -qq install -y --no-install-recommends netcat-traditional \
    && apt-get install -y --no-install-recommends gcc build-essential python3-dev \
    && rm -rf /var/lib/apt/lists/*


FROM python-base as usg
ENV DEBIAN_FRONTEND=noninteractive

ADD "$PYTHON_REQUIREMENTS_FILE" /tmp/requirements.txt
    
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    pip3 install --require-hashes -qr /tmp/requirements.txt \
    && mkdir /code

COPY certs/ca-chain.cert.pem /etc/ssl/certs/ca-chain.cert.pem
COPY certs/client.cert.pem /etc/ssl/certs/client.cert.pem
COPY certs/client.key.pem /etc/ssl/certs/client.key.pem
COPY . /code

ENTRYPOINT ["python3", "/code/core/usg.py"]
