# Changelog

All notable changes to this project are documented in this file.

## [1.0.0] - 2026-09-01

### Added

- Durable SQLite inbox/outbox with lease-based delivery and idempotent acknowledgement.
- Telegram long-polling and authenticated HTTPS webhook ingress profiles.
- Twelve allowlisted MCP tools for receiving, acknowledging and sending Telegram messages.
- Conservative `uncertain` state for outbound requests whose delivery cannot be proven.
- Strict configuration validation, secret redaction, Host/Origin protection and bounded webhook bodies.
- Reproducible hashed runtime dependency lock, Docker deployment files and GitHub Actions CI.
- Security model, operational runbook and independent audit report.

[1.0.0]: https://github.com/ph4ntom-rev/telegram-mcp-bridge/releases/tag/v1.0.0
