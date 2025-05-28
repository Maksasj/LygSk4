#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <unistd.h>
#include <time.h>

#include <mpi.h>

// Declare verbose and tag as global variables
int verbose = 0;
int tag = 0;

// Function prototypes
void Compare_and_Send(int myid, int step, int *smaller, int *gotten);
void Collect_Sorted_Sequence(int myid, int p, int smaller, int *sorted);

int main(int argc, char *argv[]) {
    int i, p, *n = NULL, j, g, s = 0;
    double start_time, end_time;

    MPI_Status status;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &p);
    MPI_Comm_rank(MPI_COMM_WORLD, &i);

    /* Manager (rank 0) generates p random numbers */
    if (i == 0) {
        n = (int *)calloc(p, sizeof(int));
        srand(time(NULL));
        for (j = 0; j < p; j++) {
            n[j] = rand() % 100;
        }

        if (verbose > 0) {
            printf("The %d numbers to sort : ", p);
            for (j = 0; j < p; j++) {
                printf(" %d", n[j]);
            }
            printf("\n");
            fflush(stdout);
        }

        start_time = MPI_Wtime();
    }

    /* Each processor i performs p-i steps of comparison and exchange */
    // This loop structure appears to be part of a sorting network logic
    for (j = 0; j < p; j++) { // This loop should likely go up to p, not p-i
        if (i == 0) { // Manager process
            g = n[j]; // Gets the j-th number

            if (verbose > 0) {
                printf("Manager (rank 0) starts with %d for step %d.\n", g, j);
                fflush(stdout);
            }

            Compare_and_Send(i, j, &s, &g);
        } else { // Worker processes
            // Workers only participate if they are part of the active comparison chain
            if (j < p - i) { // A condition to prevent reading from non-existent predecessors
                 MPI_Recv(&g, 1, MPI_INT, i - 1, tag, MPI_COMM_WORLD, &status);

                 if (verbose > 0) {
                     printf("Node %d receives %d from %d in step %d.\n", i, g, i - 1, j);
                     fflush(stdout);
                 }

                 Compare_and_Send(i, j, &s, &g);
            }
        }
    }

    MPI_Barrier(MPI_COMM_WORLD); /* Synchronize before collecting the final result */
    Collect_Sorted_Sequence(i, p, s, n);

    if(i == 0) {
        end_time = MPI_Wtime();
        printf("%d %f\n", p, end_time - start_time);
    }

    if (i == 0) {
        free(n); // Free the allocated memory
    }

    MPI_Finalize();
    return 0;
}

void Compare_and_Send(int myid, int step, int *smaller, int *gotten) {
    // In the first step (or if a process hasn't held a number yet), it keeps the received number.
    if (step == 0) {
        *smaller = *gotten;
    } else {
        if (*gotten > *smaller) {
            // If the received number is greater, send it to the next process
            MPI_Send(gotten, 1, MPI_INT, myid + 1, tag, MPI_COMM_WORLD);
            if (verbose > 0) {
                printf("Node %d keeps %d and sends %d to %d.\n", myid, *smaller, *gotten, myid + 1);
                fflush(stdout);
            }
        } else {
            // If the received number is smaller, send the old smaller number and keep the new one.
            MPI_Send(smaller, 1, MPI_INT, myid + 1, tag, MPI_COMM_WORLD);
            if (verbose > 0) {
                printf("Node %d sends %d and keeps %d.\n", myid, *smaller, *gotten);
                fflush(stdout);
            }
            *smaller = *gotten;
        }
    }
}

void Collect_Sorted_Sequence(int myid, int p, int smaller, int *sorted) {
    /* Each processor "myid" sends its final smallest number to the
     * manager (rank 0), who collects the sorted numbers. */
    MPI_Status status;
    int k;

    if (myid == 0) {
        // The manager already has its smallest number
        sorted[0] = smaller;
        // Receive the smallest number from each of the other processes
        for (k = 1; k < p; k++) {
            MPI_Recv(&sorted[k], 1, MPI_INT, k, tag, MPI_COMM_WORLD, &status);
        }

        // printf("\nThe sorted sequence: ");
        // for (k = 0; k < p; k++) {
        //     printf(" %d", sorted[k]);
        // }
        // printf("\n");

    } else {
        // All other processes send their final "smaller" value to the manager
        MPI_Send(&smaller, 1, MPI_INT, 0, tag, MPI_COMM_WORLD);
    }
}