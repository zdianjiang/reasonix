FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/reasonix ./cmd/reasonix

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/reasonix /usr/local/bin/reasonix
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/reasonix"]
