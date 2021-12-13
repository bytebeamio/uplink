# Uplink JNI for Android integration

### Setup
1. Install JRE and Android SDK
2. Generate JAVA bindings and dynamically loadable binary file:
```sh
JAVA_HOME="/path/to/java/" cargo build --release
```