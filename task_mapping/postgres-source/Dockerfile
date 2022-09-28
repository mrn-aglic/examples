FROM postgres:14-alpine AS builder

#ENV POSTGRES_HOST_AUTH_METHOD=trust
ENV POSTGRES_PASSWORD=my_super_secret_password

COPY postgres-source/postgres-init.sh /docker-entrypoint-initdb.d/postgres-init.sh

RUN ls -al /docker-entrypoint-initdb.d/

RUN mkdir -p /data
COPY dataset/soccer21-22.csv /data

RUN apk update && \
    apk add ca-certificates && \
    update-ca-certificates && \
    /usr/local/bin/docker-entrypoint.sh postgres || true && \
    cp -R /var/lib/postgresql/data/* /data



FROM postgres:14-alpine
COPY --from=builder /data /var/lib/postgresql/data
