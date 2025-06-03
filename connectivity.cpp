#include <vector>
#include <algorithm>
#include <iostream>
#include <random>
#include <cstdio>
#include <cstdlib>

#include <mpi.h>

int tag = 0;

void merge_and_split(std::vector<int>& current_data, const std::vector<int>& incoming_data, int capacity, int my_rank, int num_procs, std::vector<int>& to_send_next) {
    current_data.insert(current_data.end(), incoming_data.begin(), incoming_data.end());
    std::sort(current_data.begin(), current_data.end());

    to_send_next.clear();

    if (my_rank < num_procs - 1) {
        if (current_data.size() > static_cast<size_t>(capacity)) {
            to_send_next.assign(current_data.begin() + capacity, current_data.end());
            current_data.resize(capacity);
        }
    }
}

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);
    int my_rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Status status;

    int N = 1000;

    MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    int nominal_capacity_per_proc = 0;

    if (N > 0 && num_procs > 0)
        nominal_capacity_per_proc = (N + num_procs - 1) / num_procs;

    std::vector<int> local_sorted_data;
    std::vector<int> initial_array;

    float start_time;

    if (my_rank == 0) {
        if (N > 0) {
            initial_array.resize(N);
            srand(time(NULL));

            for (int i = 0; i < N; ++i)
                initial_array[i] = rand() % 20;
        }

        start_time = (float) MPI_Wtime();
    }

    // master
    if(my_rank == 0) {
        for (int i = 0; i < N; ++i) {
            std::vector<int> chunk_to_feed;
            chunk_to_feed.push_back(initial_array[i]);

            if (num_procs == 1) {
                local_sorted_data.insert(local_sorted_data.end(), chunk_to_feed.begin(), chunk_to_feed.end());
                std::sort(local_sorted_data.begin(), local_sorted_data.end());
            } else {
                std::vector<int> to_send_next;
                merge_and_split(local_sorted_data, chunk_to_feed, nominal_capacity_per_proc, my_rank, num_procs, to_send_next);

                if (!to_send_next.empty()) {
                    int send_size = to_send_next.size();
                    MPI_Send(&send_size, 1, MPI_INT, my_rank + 1, tag, MPI_COMM_WORLD);
                    MPI_Send(to_send_next.data(), send_size, MPI_INT, my_rank + 1, tag, MPI_COMM_WORLD);
                }
            }
        }

        if (num_procs > 1) {
            int end_signal_size = 0;
            MPI_Send(&end_signal_size, 1, MPI_INT, my_rank + 1, tag, MPI_COMM_WORLD);
        }
    } 
    
    // slave
    if (my_rank > 0) {
        bool stream_active = true;
        while (stream_active) {
            int incoming_size;
            MPI_Recv(&incoming_size, 1, MPI_INT, my_rank - 1, tag, MPI_COMM_WORLD, &status);
            
            // end
            if (incoming_size == 0) {
                stream_active = false;
               
                if (my_rank < num_procs - 1)
                    MPI_Send(&incoming_size, 1, MPI_INT, my_rank + 1, tag, MPI_COMM_WORLD);
            } else {
                std::vector<int> received_chunk(incoming_size);
                MPI_Recv(received_chunk.data(), incoming_size, MPI_INT, my_rank - 1, tag, MPI_COMM_WORLD, &status);

                std::vector<int> to_send_next;
                merge_and_split(local_sorted_data, received_chunk, nominal_capacity_per_proc, my_rank, num_procs, to_send_next);

                if (my_rank < num_procs - 1 && !to_send_next.empty()) {
                    int send_size = to_send_next.size();
                    MPI_Send(&send_size, 1, MPI_INT, my_rank + 1, tag, MPI_COMM_WORLD);
                    MPI_Send(to_send_next.data(), send_size, MPI_INT, my_rank + 1, tag, MPI_COMM_WORLD);
                }
            }
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    int my_local_data_size = local_sorted_data.size();
    std::vector<int> gather_recv_counts;
    std::vector<int> gather_displacements;
    std::vector<int> final_sorted_array;

    if (my_rank == 0)
        gather_recv_counts.resize(num_procs);

    MPI_Gather(&my_local_data_size, 1, MPI_INT, gather_recv_counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (my_rank == 0) {
        gather_displacements.resize(num_procs);
        gather_displacements[0] = 0;
        int total_elements_gathered = gather_recv_counts[0];

        for (int i = 1; i < num_procs; ++i) {
            gather_displacements[i] = gather_displacements[i-1] + gather_recv_counts[i-1];
            total_elements_gathered += gather_recv_counts[i];
        }
        
        if (N == 0 && total_elements_gathered == 0) {
            final_sorted_array.resize(0);
        } else if (total_elements_gathered != N) {
            fprintf(stderr, "Rank 0 Error: Total elements gathered (%d) does not match N (%d).\n", total_elements_gathered, N);
            final_sorted_array.resize(total_elements_gathered);
        } else {
            final_sorted_array.resize(N);
        }

        MPI_Gatherv(local_sorted_data.data(), my_local_data_size, MPI_INT, final_sorted_array.data(), gather_recv_counts.data(), gather_displacements.data(), MPI_INT, 0, MPI_COMM_WORLD);

        float end_time = MPI_Wtime();
        printf("%d %f\n", num_procs, end_time - start_time);

    } else {
        MPI_Gatherv(local_sorted_data.data(), my_local_data_size, MPI_INT, NULL, NULL, NULL, MPI_INT, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}