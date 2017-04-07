sh stop.sh
mkdir -p store1
cp ./target/debug/tikv-server ./store1
cd store1
rm -rf ./data
rm -rf ./log
mkdir -p ./data/volumes
RUST_BACKTRACE=full ./tikv-server -C config.toml &
cd ..

sleep 5

mkdir -p store2
cp ./target/debug/tikv-server ./store2
cd store2
rm -rf ./data
rm -rf ./log
mkdir -p ./data/volumes
RUST_BACKTRACE=full ./tikv-server -C config.toml &
cd ..



mkdir -p store3
cp ./target/debug/tikv-server ./store3
cd store3
rm -rf ./data
rm -rf ./log
mkdir -p ./data/volumes
RUST_BACKTRACE=full ./tikv-server -C config.toml &
cd ../
