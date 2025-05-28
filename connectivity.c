#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <unistd.h>
#include <time.h>
#include <mpi.h>

#define MASTER_PROCESS_ID 0

int tag = 0;

void pass_next(int cpid, int step, int *old, int *new) {
    if (step == 0) {
        *old = *new;
        return;
    }

    if (*new > *old)
        MPI_Send(new, 1, MPI_INT, cpid + 1, tag, MPI_COMM_WORLD);
    else {
        MPI_Send(old, 1, MPI_INT, cpid + 1, tag, MPI_COMM_WORLD);
        *old = *new;
    }
}

void get_result_array(int cpid, int job_size, int old, int *sorted) {
    MPI_Status status;

    if(cpid != MASTER_PROCESS_ID) {
        MPI_Send(&old, 1, MPI_INT, 0, tag, MPI_COMM_WORLD);
        return;
    }

    sorted[0] = old;

    for (int k = 1; k < job_size; k++)
        MPI_Recv(&sorted[k], 1, MPI_INT, k, tag, MPI_COMM_WORLD, &status);
}

int main(int argc, char *argv[]) {
    srand(time(NULL));

    int process_rank;
    int job_size;

    MPI_Status status;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &job_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

    float start_time;

    int* array = NULL;
    int new_number;
    int sorted_number = 0;

    if (process_rank == MASTER_PROCESS_ID) {
        array = (int *)calloc(job_size, sizeof(int));

        for (j = 0; j < job_size; j++)
            array[js] = rand() % 100;

        start_time = (float) MPI_Wtime();
    }

    for (int i = 0; i < job_size; i++) {
        if (process_rank == MASTER_PROCESS_ID) {
            new_number = array[i];
            pass_next(process_rank, i, &sorted_number, &new_number);
            continue;
        }

        if (j < job_size - process_rank) {
            MPI_Recv(&new_number, i, MPI_INT, process_rank - 1, tag, MPI_COMM_WORLD, &status);
            pass_next(process_rank, i, &sorted_number, &new_number);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    get_result_array(process_rank, job_size, sorted_number, array);

    if(process_rank == MASTER_PROCESS_ID) {
        float end_time = MPI_Wtime();
        printf("%d %f\n", job_size, end_time - start_time);

        free(array);
    }

    MPI_Finalize();

    return 0;
}
