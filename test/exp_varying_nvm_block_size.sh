#!/bin/bash

for SZ in 64 256 512 1024
do
cat << EOF > include/config.h
#ifndef SPITFIRE_CONFIG_H
#define SPITFIRE_CONFIG_H
namespace spitfire {

constexpr size_t kNVMBlockSize = ${SZ};

}
#endif //SPITFIRE_CONFIG_H
EOF
make
done