FROM mysql:8.0-debian AS mysql-builder

#ENV POSTGRES_HOST_AUTH_METHOD=trust
#ENV POSTGRES_PASSWORD=my_super_secret_password
ENV MYSQL_ROOT_PASSWORD=my-secret-pw

RUN mkdir -p /data
RUN mkdir -p /init
COPY dataset/ratings_smaller.csv /data

RUN apt-get -y update && apt-get upgrade -y && \
    apt-get install ca-certificates &&  \
    update-ca-certificates

COPY mysql-sink/init.sql /init/init.sql
COPY mysql-sink/mysql-init.sh /docker-entrypoint-initdb.d/mysql-init.sh

RUN /usr/local/bin/docker-entrypoint.sh mysqld --secure-file-priv="/data/"
#    cp -R /var/lib/mysql/data/* /data



FROM mysql-builder as mysql-sink
