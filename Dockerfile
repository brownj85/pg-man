#syntax=docker/dockerfile:1.3.0-labs

ARG UID=1000
ARG GID=1000
ARG USER=dbman
ARG USER_HOME=/var/lib/${USER}
ARG LIB_PATH=${USER_HOME}/lib

FROM alpine AS fetch-apgdiff
ARG APGDIFF_DOWNLOAD_URL="https://github.com/fordfrog/apgdiff/archive/refs/tags/release_2.7.0.zip"

WORKDIR /

RUN wget ${APGDIFF_DOWNLOAD_URL} && \
    unzip $(basename ${APGDIFF_DOWNLOAD_URL}) && \
    mv apgdiff-release_2.7.0/releases/apgdiff-2.7.0.jar /apgdiff-2.7.0.jar

FROM python:3.13-slim-bookworm AS setup-user
ARG UID
ARG GID
ARG USER
ARG USER_HOME

RUN groupadd \
        --force \
        --gid ${GID} \
        ${USER} \
    && useradd \
        --create-home \
        --gid ${GID} \
        --uid ${UID} \
        --home-dir ${USER_HOME} \
        ${USER}

USER ${UID}:${GID}
WORKDIR ${USER_HOME}

FROM setup-user AS install-lib
ARG UID
ARG GID
ARG USER
ARG USER_HOME

COPY ./src ${USER_HOME}/src
COPY --chown=${UID}:${GID} pyproject.toml uv.lock README.md ${USER_HOME}

RUN pip install --target ${USER_HOME}/lib .

FROM setup-user AS runtime
ARG UID
ARG GID
ARG USER
ARG USER_HOME

USER root
RUN apt-get update -y && \
    apt-get install -y openjdk-17-jre postgresql-common && \
    /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y && \
    apt-get update -y && \
    apt-get install -y postgresql-client-16 postgresql-16 && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

USER ${UID}:${GID}
ENV APGDIFF_JAR_PATH=${USER_HOME}/lib/apgdiff-2.7.0.jar
ENV PYTHONPATH=${USER_HOME}/lib
ENV PATH=${USER_HOME}/lib/bin:${PATH}

COPY --from=install-lib --chown=${UID}:${GID} ${USER_HOME}/lib ${USER_HOME}/lib
COPY --from=fetch-apgdiff --chown=${UID}:${GID} /apgdiff-2.7.0.jar ${USER_HOME}/lib/apgdiff-2.7.0.jar

ENTRYPOINT ["dbman"]