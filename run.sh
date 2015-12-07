make; make clean all
./generate 4194304
mpirun -n 8 -machinefile mfile run
