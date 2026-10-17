# cockatiel_module-range-rs

A tiny Cockatiel module: a random number in an explicit range.

## Command
- `!range` — a number in `1..=100`
- `!range 20` — a number in `1..=20`
- `!range 5 10` — a number in `5..=10` (inclusive)

A reversed range (`!range 10 5`) is swapped to `5..=10`.
Reply example: `🎲 bob 5–10: 8`.

## Config (`config.json` → `module_specific`)
- `command_flag` — the command prefix, default `!`
- `reconnect_base_secs` / `reconnect_max_secs` — reconnect backoff bounds

## Build
```sh
cargo build --release
```
