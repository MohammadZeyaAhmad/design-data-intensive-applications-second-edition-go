FROM eclipse-temurin@sha256:1eac8b8fee11ca7d6725a48bc5d41992ed1561504ebae25490d2d062847e8269
WORKDIR /app
COPY fuseki-server.jar /app/fuseki-server.jar
COPY --chmod=0755 health entrypoint.sh /app/
RUN mkdir /data && chown 10001:10001 /data
USER 10001:10001
ENTRYPOINT ["/app/entrypoint.sh"]
