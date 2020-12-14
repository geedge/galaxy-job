ARG JDK_IMAGE
FROM ${JDK_IMAGE}
MAINTAINER Galaxy
VOLUME /tmp
WORKDIR /home/tsg/galaxy/galaxy-job/galaxy-job-executor
COPY config config
ARG JAR_FILE
COPY ${JAR_FILE} galaxy-job-executor.jar
#dockercompose set JAVA_OPTS
ENV JAVA_OPTS=" -Xms1024m -Xmx2048m "
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8
#ENV TZ=Asia/Almaty
#RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar galaxy-job-executor.jar" ]
