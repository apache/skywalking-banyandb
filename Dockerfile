FROM ubuntu:latest
LABEL authors="zhou"

ENTRYPOINT ["top", "-b"]