#!/usr/bin/env bash
# This script will try to disable CPU frequency changes, and disable c-states in
# Linux.
# Credit to Fabricio Carvalho.
CPU_FREQ_KHZ=3300000
RDMSR=$(which rdmsr)
WRMSR=$(which wrmsr)
set_freq() {
        if [ -d /sys/devices/system/cpu/cpu0/cpufreq/ ]; then
                for i in $(ls /sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq); do echo "${CPU_FREQ_KHZ}" | sudo tee $i > /dev/null 2>&1 ;done
                for i in $(ls /sys/devices/system/cpu/cpu*/cpufreq/scaling_min_freq); do echo "${CPU_FREQ_KHZ}" | sudo tee $i > /dev/null 2>&1 ;done
        fi
}
disable_cstate() {
        echo "Disabling C-states"
        for i in $(ls /sys/devices/system/cpu/cpu*/cpuidle/state*/disable); do echo "1" | sudo tee $i > /dev/null 2>&1 ;done
}
disable_turbo() {
        if ! [ -x "$(command -v ${RDMSR})" ]; then
                echo "Installing msr-tools ..."
                sudo apt install msr-tools
        fi
        if [ -z "$(lsmod | grep '^msr')" ]; then
                echo "Loading msr module"
                sudo modprobe msr
        fi
        TURBO_BOOST_BIT=38
        echo "Disabling Turbo Boost"
        sudo ${WRMSR} -a 0x1a0 $(printf "0x%x" $(($(sudo ${RDMSR} -d 0x1a0)|(1<<${TURBO_BOOST_BIT}))))
}
set_freq;
disable_cstate;
#disable_turbo;
