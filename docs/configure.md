# Configuring `uplink`
One may configure certain features of uplink with the help of a `config.toml` file by using the commandline arguments `-c` or `--config`:
```sh
uplink -a auth.json -c config.toml
```

It must be noted that parts of, or the entirety of the config file is optional and a user may choose to omit it, letting uplink default to configuration values that are compiled into the binary. uplink only expects the `config.toml` to contain configuration details as given in the [example config.toml][config] file in the configs folder.

## Configurign uplink *built-in*s

# File Downloader
Uplink has an in-built file downloader which can handle special **downloader actions** that contain payload with information relating to the file to be downloaded, such as URL, .

[config]: configs/config.toml