lib:
	cargo build

java_run: lib
	cd examples && javac Uplink.java && java -Djava.library.path=../../target/debug Uplink