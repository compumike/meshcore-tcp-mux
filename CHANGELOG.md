# Changelog

---

## Unreleased

- `MeshCoreTCPMux::Broker.fan_out_dedicated`: reduce log level to debug for dedicated-client inbox overflow events
- Reorder Dockerfile for better caching / faster builds. In Makefile, use threaded Crystal compilation

---

## 1.1.3

- Upgrade Alpine container base image

---

## 1.1.2

- ProcessAlarmWatchdog to kill if process gets stuck or can't reconnect upstream within 5 minutes

---

## 1.1.1

- Updated documentation, README.md, etc.

---

## 1.1.0

- `--deduplicate-received-messages` feature: don't forward received channel or DM texts with different attempt numbers
- Move some noisy logging to LOG_LEVEL=DEBUG

---

## 1.0.0

- Initial release
