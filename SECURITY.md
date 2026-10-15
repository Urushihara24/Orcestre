# Security Policy

## Scope

Security reports are welcome for issues involving the bot's Telegram administration surface, Epic authorization material, persisted `device_auth`, PostgreSQL state, outbound friend-request controls, or deployment configuration.

Examples include:

- exposure of Telegram/Epic credentials or `device_auth`;
- bypass of administrator or auth-operator restrictions;
- outbound friend requests occurring while `DRY_RUN` or `SEND_REQUESTS_ENABLED` should block them;
- unsafe account/target state transitions that could trigger unintended operations;
- dependency, container, or persistence issues that create a security boundary failure.

## Reporting

Do **not** publish bot tokens, Epic credentials, account credentials, `device_auth`, database connection strings, proxy credentials, or exploit details in a public issue.

Use GitHub private vulnerability reporting when it is available for this repository. If a private GitHub report is not available, send a sanitized summary through the work contact listed on the maintainer profile and keep secrets out of the initial message.

Useful report details include:

- affected commit/version and execution mode;
- minimal reproduction steps with placeholder credentials;
- expected vs actual safety behavior;
- whether the issue can create outbound requests or alter persisted account/target state;
- sanitized logs or screenshots.

## Operational safety

`DRY_RUN` and `SEND_REQUESTS_ENABLED` are deliberate outbound-operation boundaries. Security fixes must preserve fail-safe behavior and must not weaken administrator/auth-operator access controls.

Never commit `.env`, Telegram/Epic secrets, account credential imports, `device_auth`, production database URLs, or proxy credentials.