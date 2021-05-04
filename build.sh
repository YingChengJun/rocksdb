rm -rf build
mkdir build && cd build
cmake .. -DWITH_SNAPPY=1
make -j32