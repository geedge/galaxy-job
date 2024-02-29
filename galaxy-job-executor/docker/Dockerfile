ARG JDK_IMAGE
ARG GO_IMAGE
#编译环境，生成二进制文件
#FROM 192.168.40.153:9080/common/golang:1.15.6 as builder
FROM ${GO_IMAGE} as builder

WORKDIR /build
COPY xjar.go /build/

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o xjar .

#FROM 192.168.40.153:9080/common/jdk:1.8.0_73-jre
FROM ${JDK_IMAGE}
MAINTAINER Galaxy
VOLUME /tmp
WORKDIR /opt/galaxy-job-executor
COPY --from=builder /build .

COPY config config
ARG JAR_FILE
COPY ${JAR_FILE} galaxy-job-executor.xjar
RUN mkdir -p /usr/lib/jvm && ln -s /opt/jdk/ /usr/lib/jvm/jdk1.8.0_73

#dockercompose set JAVA_OPTS
ENV JAVA_OPTS=" -Xms1024m -Xmx2048m "
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8
#ENV TZ=Asia/Almaty
#RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
ENTRYPOINT [ "sh", "-c", "./xjar java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar galaxy-job-executor.xjar" ]