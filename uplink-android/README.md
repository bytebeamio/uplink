# Uplink JNI for Android integration

### Setup
1. Install JRE, Android SDK along with NDK-Bundle component with the help of SDK manager. Use locations in the same to set values in .cargo/config.
2. Using the following commandline tool seems to be necessary, but might be avoided according to itself:
```sh
make_standalone_toolchain.py --api 29 --arch x86 --install-dir NDK/x86/
```
You must use the ar and linker that are hence generated in above mentioned folder.
3. Generate JAVA bindings and dynamically loadable binary file:
```sh
JAVA_HOME="/path/to/java/" ./gradlew build
```
4. Use the generated jar file(in `./build/libs`) as a library in your app.