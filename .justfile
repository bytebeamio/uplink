v:
  cargo run -- -a configs/noauth.json -c configs/config.toml -v

vv:
  cargo run -- -a configs/noauth.json -c configs/config.toml -vv

stress:
  cargo run -- -a configs/stress.json -c configs/stress.toml -m uplink::base::serializer -v
