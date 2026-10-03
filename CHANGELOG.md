# Changelog

---

## Unreleased

- Companion protocol version compatibility: handle v14's new RUN_CLI_COMMAND command and response, and advertise v14 support to upstream companion.
- Companion protocol version compatibility: support newer upstream versions going forward, and support v13 and v14 on a per-client basis.
- (none)

---

## 1.2.0

- Fix contact updates from MeshCore One, including saving direct routing, by accepting its three reserved padding bytes.

---

## 1.1.4

- `MeshCoreTCPMux::Broker.fan_out_dedicated`: reduce log level to debug for dedicated-client inbox overflow events
- Reorder Dockerfile for better caching / faster builds. In Makefile, use threaded Crystal compilation
- Fallback to support pre-1.19 Crystal `Time.monotonic` instead of newer `Time.instant` (thanks @enigmaspb)

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
