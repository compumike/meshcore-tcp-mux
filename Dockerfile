# syntax=docker/dockerfile:1

# Pin multi-platform index digests, not a single architecture's manifest.
FROM crystallang/crystal:1.21.0-alpine@sha256:82ed00e2f1d0d45267c76e45def3c97e4c2ef20bf8005067ab0124a393f610fc AS build
RUN apk add --no-cache bash make
WORKDIR /app
COPY Makefile ./
COPY src/ ./src/
# Build natively for each target platform (or through BuildKit emulation).
# A generic CPU target avoids requiring the build host's instruction extensions.
RUN mkdir -p spec && make BUILD_FLAGS="--release --static --mcpu generic"

FROM build AS test
# Test dependencies never enter the runtime image. No hardware is contacted:
# both the Crystal specs and Python smoke check use isolated fake companions.
RUN apk add --no-cache python3 py3-pip \
    && python3 -m venv /opt/test-env \
    && /opt/test-env/bin/pip install --no-cache-dir meshcore==2.3.9.1 meshcore-cli==1.6.3
COPY spec/ ./spec/
COPY scripts/check_fake_clients.py scripts/check_live_reconnect.py scripts/check_clients.py scripts/test_readiness.py ./scripts/
RUN make format-check spec smoke PYTHON=/opt/test-env/bin/python

FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40 AS runtime
RUN addgroup -S -g 10001 mux && adduser -S -D -H -u 10001 -G mux mux
# Copying from test makes successful tests a prerequisite for the final image.
COPY --from=test /app/out/meshcore-tcp-mux /usr/local/bin/meshcore-tcp-mux
COPY LICENSE /usr/share/licenses/meshcore-tcp-mux/LICENSE
USER 10001:10001
RUN /usr/local/bin/meshcore-tcp-mux --help > /dev/null
EXPOSE 5001/tcp
STOPSIGNAL SIGTERM
# Direct exec preserves CLI arguments and delivers SIGTERM to the existing handler.
ENTRYPOINT ["/usr/local/bin/meshcore-tcp-mux"]
CMD ["--help"]
