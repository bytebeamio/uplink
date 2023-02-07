v:
  cargo run -- -a configs/noauth.json -c configs/config.toml -v

vv:
  cargo run -- -a configs/noauth.json -c configs/config.toml -vv

stage:
  cargo run -- -a configs/stress.json -c configs/stress.toml -v

stress:
  cargo run -- -a configs/stress.json -c configs/stress.toml -m uplink::base::serializer -v
