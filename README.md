# AutoFuelPrice

Twice-daily Bangchak fuel price watcher with LINE push notifications.

## What it does

1. Triggers at **18:00 and 20:00 Asia/Bangkok** every day.
2. Pulls retail fuel prices from `https://oil-price.bangchak.co.th/apioilprice2/en`.
3. Compares with the most recent snapshot (stored on disk).
4. If any price (today or tomorrow) changed, pushes a LINE message.

State is persisted as a single JSON file (`last_fuel_prices.json` by default).
No database, no HTTP server, no dependencies beyond the binary itself.

## Setup

### Prerequisites

- Rust toolchain (edition 2024). Tested on `rustc 1.95.0`.

### Configure

```sh
cp .env.example .env
# then edit .env to add LINE_CHANNEL_TOKEN and LINE_TARGET_ID
```

| Variable             | Required | Default                       | Purpose                                              |
| -------------------- | -------- | ----------------------------- | --------------------------------------------------- |
| `LINE_CHANNEL_TOKEN` | yes\*    | _(empty)_                     | LINE Messaging API channel access token.            |
| `LINE_TARGET_ID`     | yes\*    | _(empty)_                     | User/group/room ID to receive push messages.        |
| `STATE_FILE`         | no       | `./last_fuel_prices.json`     | Where the last-known price snapshot is stored.      |
| `RUN_ONCE`           | no       | _(unset)_                     | If set, runs one tick and exits (no scheduler).     |
| `RUST_LOG`           | no       | `info`                        | `tracing_subscriber` filter directive.              |

\* The bot runs without LINE credentials (logs warnings instead of pushing),
so you can verify the fetch/diff path before credentials are provisioned.

### Run (built-in scheduler)

```sh
cargo run --release
```

The process stays alive, fires twice a day, and persists snapshots between
runs. Send `SIGINT` (Ctrl+C) to shut down cleanly.

### Run (external scheduler, optional)

If you prefer `cron` or `systemd` timers, build once and invoke with
`RUN_ONCE=1`:

```sh
cargo build --release
RUN_ONCE=1 ./target/release/auto_fuel_price
```

Sample crontab (Bangkok time on the host):

```cron
# m h  dom mon dow   command
0  18 * * *   RUN_ONCE=1 /path/to/auto_fuel_price
0  20 * * *   RUN_ONCE=1 /path/to/auto_fuel_price
```

## Validation

```sh
cargo check        # type-check
cargo clippy --all-targets -- -D warnings
cargo fmt --check
cargo test         # unit tests for diff + message formatting
```

Manual smoke test (no LINE credentials required):

```sh
STATE_FILE=/tmp/test-prices.json RUN_ONCE=1 cargo run --release
cat /tmp/test-prices.json
# Re-run to confirm "no price changes detected"
STATE_FILE=/tmp/test-prices.json RUN_ONCE=1 cargo run --release
```

## LINE message format

```
⛽ Fuel price update (21/06/2026)
• Gasohol 95: 31.00 Baht/L (tomorrow: 30.50)
• Diesel B20: 32.50 Baht/L (tomorrow: 32.50)
```

Only entries whose today-price or tomorrow-price changed are listed.

## Notes

- Schedule is encoded in UTC inside the binary (`0 0 11,13 * * *`) since
  Bangkok is UTC+7 with no DST. If you deploy with an external scheduler,
  use local-time cron expressions instead.
- First run has no previous snapshot — every current price is treated as
  "changed" so the initial LINE message lists everything once.
- `oil-price.bangchak.co.th` returns prices for the next effective date;
  the bot trusts the API's `OilPriceDate` for display.
