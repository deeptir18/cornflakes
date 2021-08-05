#!/bin/bash

RESULTS=$1
BINARY=$PWD/../scatter-gather-bench/build/netperf
PLOT=$PWD/../experiments/plotting_scripts/memcpy_bench.R
PCIADDR=$2 # TODO: think of a better way to do this

mkdir -p $RESULTS
logfile="${RESULTS}/summary.log"
mkdir $RESULTS/plots
TRIALS=5
touch $logfile
# echo "array_size,segment_size,with_copy,time" >> $logfile

arraySizes=(16384 49152 163840 491520 4096000 8192000 16384000 49152000 65536000)
segmentSizes=(1024 4096 8192 16384)

for seg in ${segmentSizes[@]}; do
   $PLOT $logfile $seg $RESULTS/plots/segsize_${seg}.pdf "by_array_size"
done
exit

for array in ${arraySizes[@]}; do
    for seg in ${segmentSizes[@]}; do
        for trial in $(seq 1 $TRIALS); do
            echo "Running trial array size ${array}, segment size $seg, trial $trial"
            logprefix_read=${RESULTS}/arraysize_${array}/segsize_${seg}/read/trial_${trial}
            logprefix_memcpy=${RESULTS}/arraysize_${array}/segsize_${seg}/memcpy/trial_${trial}
            mkdir -p $logprefix_read
            mkdir -p $logprefix_memcpy
            # run binary without copy
            #sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ${BINARY} -c 0xff -n 8 -w \
            #    $PCIADDR,txq_inline_mpw=256,txqs_min_inline=0 --proc-type=auto -- --mode=MEMCPY --segment_size=$seg --array_size=$array > \
             #   ${logprefix_read}/stdout.log 2> ${logprefix_read}/stderr.log
            # run binary with copy
            sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ${BINARY} -c 0xff -n 8 -w \
                $PCIADDR,txq_inline_mpw=256,txqs_min_inline=0 --proc-type=auto -- --mode=MEMCPY --segment_size=$seg --array_size=$array --with_copy > \
                ${logprefix_memcpy}/stdout.log 2> ${logprefix_memcpy}/stderr.log
                cat ${logprefix_read}/stdout.log | tail -n1 >> $logfile
                cat ${logprefix_memcpy}/stdout.log | tail -n1 >> $logfile
        done
    done
done

for seg in ${segmentSizes[@]}; do
   $PLOT $logfile $RESULTS/plots/segsize_${seg}.pdf "by_array_size" $seg
done

