FROM scratch
COPY --chmod=0555 bin/atlas /atlas
USER 65532:65532
ENTRYPOINT ["/atlas"]
