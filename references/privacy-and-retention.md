# Privacy and retention

The default policy stores full visible user prompts and bounded assistant text in `.context-ledger/ledger.sqlite3`. That data may contain secrets, personal information, or proprietary material.

Before enabling hooks:

- review `.context-ledger/config.json`;
- add redaction patterns appropriate to the project;
- choose metadata-only or disabled capture for tool content that should not persist;
- keep `.context-ledger/` out of version control and unintended backups;
- apply filesystem permissions and backup policy appropriate to the data.

Structured keys resembling credentials, tokens, passwords, cookies, secrets, or private keys are redacted when structured tool content is retained. Regex redaction occurs before event insertion.

The live event table can be bounded with verified prefix archives. Archives are compressed JSONL plus metadata and checksums; they remain sensitive and part of the audit history. `export-audit` reconstructs a complete JSONL stream from archives plus live rows.

Removing already-captured content is an operator-controlled rotation or replacement operation. This package does not provide encryption at rest, access control, secure deletion, authenticity signatures, or protection from an actor that can rewrite all project files.
