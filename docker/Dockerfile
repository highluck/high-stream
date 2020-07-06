FROM registry.navercorp.com/ncc-base/centos7/jdk:11.0.2.x64

RUN mkdir -p /home1/irteam/deploy
ADD consumer/promotion-consumer/target/promotion-consumer.jar /home1/irteam/deploy

USER root
RUN chown -R irteam:irteam /home1/irteam/deploy

USER irteam
RUN mkdir -p /home1/irteam/logs/promotion-consumer

WORKDIR /home1/irteam
ENTRYPOINT exec /home1/irteam/apps/jdk/bin/java -jar /home1/irteam/deploy/promotion-consumer.jar
