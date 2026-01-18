############################################################################################
####  SERVER
############################################################################################

FROM clux/muslrust:stable AS chef
USER root
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY ./pentaract .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Build deps (cache)
RUN cargo chef cook --release --target aarch64-unknown-linux-musl --recipe-path recipe.json

# Build app
COPY ./pentaract .
RUN cargo build --release --target aarch64-unknown-linux-musl

RUN cp /app/target/aarch64-unknown-linux-musl/release/pentaract /app/pentaract

############################################################################################
####  UI
############################################################################################

FROM node:21-slim AS ui
WORKDIR /app
COPY ./ui .

RUN npm install -g pnpm
RUN pnpm i

ENV VITE_API_BASE=/api
RUN pnpm run build

############################################################################################
####  RUNNING
############################################################################################

FROM scratch AS runtime

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /app/pentaract /pentaract

COPY --from=ui /app/dist /ui

ENTRYPOINT ["/pentaract"]
