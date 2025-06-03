#!/bin/bash
#SBATCH -p main
#SBATCH -n256
module load openmpi
mpic++ -o connectivity connectivity.cpp
mpirun -np 1 ./connectivity
mpirun -np 2 ./connectivity
mpirun -np 4 ./connectivity
# mpirun -np 8 ./connectivity
# mpirun -np 16 ./connectivity
# mpirun -np 32 ./connectivity
# mpirun -np 64 ./connectivity
# mpirun -np 128 ./connectivity
# mpirun -np 256 ./connectivity