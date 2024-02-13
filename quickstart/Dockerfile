FROM flink:1.17

COPY . .
RUN mkdir /opt/flink/usrlib
ADD target/quickstart-0.1.jar /opt/flink/usrlib/quickstart-0.1.jar

RUN chown -R flink:flink /opt/flink