# Using image without lein for deployment.
FROM amazoncorretto:21
LABEL maintainer="Tristan Nelson <thnelson@geisinger.edu>"

COPY target/app.jar /app/app.jar

EXPOSE 8888

CMD ["java", "-jar", "/app/app.jar"]
