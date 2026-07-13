FROM golang:1 AS builder

COPY . /app
WORKDIR /app

RUN make build

FROM gcr.io/distroless/static-debian13

COPY --from=builder /app/bin/service-leader-elector /service-leader-elector

ENTRYPOINT ["/service-leader-elector"]

CMD ["serve"]
