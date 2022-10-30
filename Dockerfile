FROM alpine:latest
COPY . /home
WORKDIR /home
RUN bash