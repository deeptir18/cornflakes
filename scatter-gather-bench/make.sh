#!/bin/bash
make clean; PKG_CONFIG_PATH=$LD_LIBRARY_PATH/pkgconfig DEBUG=n TIMERS=y make
