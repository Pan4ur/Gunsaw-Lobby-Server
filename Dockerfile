FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gunsaw-lobby .

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/gunsaw-lobby /gunsaw-lobby
EXPOSE 8080/tcp 27015/udp
ENTRYPOINT ["/gunsaw-lobby"]
