v:
  cargo run -- -a configs/noauth.json -c configs/config.toml -v

vv:
  cargo run -- -a configs/noauth.json -c configs/config.toml -vv

stress:
  cargo run -- -a configs/toxic.json -c configs/simulator.toml -vv
