FROM golang:1.22
LABEL authors="volume999"

WORKDIR /app

COPY . .

RUN go mod download

RUN go test -bench=. -benchtime=5s -cpu=8 -timeout=0 -benchmem > outputFile.txt