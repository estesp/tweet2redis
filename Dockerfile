FROM golang:1.7-alpine
MAINTAINER Phil Estes <estesp@gmail.com>

# You must set these keys before running the application
# pass them to `docker run -e TWITTER_...` or with
# --env-file
ENV TWITTER_CONSUMER_KEY ""
ENV TWITTER_CONSUMER_SECRET ""
ENV TWITTER_ACCESS_TOKEN ""
ENV TWITTER_ACCESS_SECRET ""

ENV REDIS_HOST localhost
ENV REDIS_PORT 6379
ENV SRCDIR /go/src/github.com/estesp/tweet2redis

RUN mkdir -p ${SRCDIR}

WORKDIR ${SRCDIR}
ENV GOPATH /go:${SRCDIR}
ADD . ${SRCDIR}
RUN go build -o tweet2redis .

ENTRYPOINT [ "/go/src/github.com/estesp/tweet2redis/tweet2redis" ]
