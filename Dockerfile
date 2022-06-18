FROM golang:1.18-buster AS build
RUN apt-get update && apt-get install git
RUN git clone https://github.com/asynkron/protoactor-go /protoactor-go
WORKDIR /build
COPY . .
RUN mkdir target
RUN go build -o target/andagdad ./cmd/andagda/server
RUN go build -o target/andagda ./cmd/andagda/client

FROM debian:stable-slim
COPY --from=build /build/target/* /usr/bin