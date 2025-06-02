#include <mpi.h>
#include <vector>
#include <algorithm> // std::sort, std::max
#include <iostream>  // std::cout, std::cerr, std::endl (not used if only printf)
#include <random>    // std::random_device, std::mt19937, std::uniform_int_distribution
#include <cstdio>    // printf, fprintf, fflush
#include <cstdlib>   // std::atoi

// Global verbose flag (controlled by command-line argument)
// Level 0: Quiet (only final status)
// Level 1: Basic signals and final array summary
// Level 2: Detailed send/receive actions
int verbose = 0;

// MPI Tags
const int DATA_TAG = 0; // Tag for data messages (size and content)
// An end-of-stream signal is indicated by sending a message size of 0 using DATA_TAG.

/**
 * @brief Merges incoming data with current data, sorts, and splits if over capacity.
 *
 * This function is called by each process in the pipeline. It takes the process's
 * current sorted data, adds new incoming data, and re-sorts.
 * If the process is not the last one in the pipeline and its data exceeds its
 * nominal 'capacity', the excess elements (the largest ones) are prepared to be
 * sent to the next process (stored in 'to_send_next').
 * The last process in the pipeline does not split its data; it keeps all elements
 * it accumulates, so 'to_send_next' will remain empty for it.
 *
 * @param current_data Reference to the vector holding the process's current sorted data.
 * This vector is modified in place.
 * @param incoming_data A vector containing data received from the previous process
 * or the initial feed from the manager.
 * @param capacity The target number of elements this process should ideally hold if it's
 * not the last process in the pipeline.
 * @param my_rank Rank of the current MPI process.
 * @param num_procs Total number of MPI processes.
 * @param to_send_next Reference to a vector where elements to be sent to the next
 * process will be stored. Cleared and filled by this function.
 */
void merge_and_split(
    std::vector<int>& current_data,
    const std::vector<int>& incoming_data,
    int capacity,
    int my_rank,
    int num_procs,
    std::vector<int>& to_send_next)
{
    // Append incoming data to current_data
    current_data.insert(current_data.end(), incoming_data.begin(), incoming_data.end());
    // Sort the combined data
    std::sort(current_data.begin(), current_data.end());

    to_send_next.clear(); // Ensure it's empty before potentially filling

    // Only split and prepare data for sending if not the last process in the pipeline
    if (my_rank < num_procs - 1) {
        // Check if the current data size exceeds the nominal capacity for this process
        if (current_data.size() > static_cast<size_t>(capacity)) {
            // Elements to send are those beyond the 'capacity' limit
            to_send_next.assign(current_data.begin() + capacity, current_data.end());
            // Retain only the first 'capacity' elements in current_data
            current_data.resize(capacity);
        }
    }
    // The last process (my_rank == num_procs - 1) keeps all data it accumulates.
    // Its current_data vector will grow and remain sorted. It does not send data further.
}


int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);
    int my_rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Status status;

    int N = 0; // Total number of elements to sort

    // Argument parsing (only by rank 0, then broadcast)
    if (my_rank == 0) {
        if (argc > 1) {
            N = std::atoi(argv[1]);
        } else {
            N = 10000; // Default N if not provided
            if (verbose) printf("Rank 0: N not provided, using default N = %d\n", N);
        }
        if (argc > 2) {
            verbose = std::atoi(argv[2]);
        } else {
            // verbose remains 0 if not specified
        }
    }

    // Broadcast N and verbose from rank 0 to all other processes
    MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&verbose, 1, MPI_INT, 0, MPI_COMM_WORLD);

    // Validate N. If N is not positive, exit.
    // (num_procs > 0 is a sanity check, should always be true after MPI_Init)
    if (N <= 0 && num_procs > 0) {
        if (my_rank == 0) {
            fprintf(stderr, "Error: Number of elements N must be positive. N = %d\n", N);
        }
        MPI_Finalize();
        return 1;
    }
    
    // Calculate the nominal capacity for each process (except possibly the last).
    // This is ceil(N/num_procs).
    int nominal_capacity_per_proc = 0;
    if (N > 0 && num_procs > 0) {
        nominal_capacity_per_proc = (N + num_procs - 1) / num_procs;
    }
    // If N is 0, capacity is also 0. All data vectors will remain empty.

    std::vector<int> local_sorted_data; // Stores the sorted data portion for this process

    std::vector<int> initial_array;

    float start_time;

    if (my_rank == 0) {
        if (N > 0) { // Only generate data if N is positive
            initial_array.resize(N);
            // Use C++ random number generation for better quality randomness
            std::random_device rd;
            std::mt19937 gen(rd()); // Seed the generator
            // Generate numbers in a range, e.g., 1 to N*10, to get varied values
            std::uniform_int_distribution<> distrib(1, N * 20);
            for (int i = 0; i < N; ++i) {
                initial_array[i] = distrib(gen);
            }

            if (verbose) {
                printf("Rank 0: Initial unsorted array (%d elements): ", N);
                for (int k=0; k<std::min(N, 20); ++k) printf("%d ", initial_array[k]);
                printf("\n");
                fflush(stdout);
            }
        }

        start_time = (float) MPI_Wtime();
    }

    // Rank 0: Generates data and feeds it into the pipeline
    if(my_rank == 0) {
        // Manager (rank 0) feeds data into the pipeline, one element at a time.
        // This models the "value by value" flow.
        for (int i = 0; i < N; ++i) {
            std::vector<int> chunk_to_feed;
            chunk_to_feed.push_back(initial_array[i]); // Create a chunk of one element

            if (num_procs == 1) {
                // If only one process, it does all the work locally.
                local_sorted_data.insert(local_sorted_data.end(), chunk_to_feed.begin(), chunk_to_feed.end());
                std::sort(local_sorted_data.begin(), local_sorted_data.end());
            } else {
                // Rank 0 acts as the first stage of the pipeline.
                std::vector<int> to_send_next;
                merge_and_split(local_sorted_data, chunk_to_feed, nominal_capacity_per_proc, my_rank, num_procs, to_send_next);

                // If there's data to send to the next process (rank 1)
                if (!to_send_next.empty()) {
                    int send_size = to_send_next.size();
                    MPI_Send(&send_size, 1, MPI_INT, my_rank + 1, DATA_TAG, MPI_COMM_WORLD);
                    MPI_Send(to_send_next.data(), send_size, MPI_INT, my_rank + 1, DATA_TAG, MPI_COMM_WORLD);
                    if (verbose > 1) { // More detailed logging for verbose level 2+
                        printf("Rank %d (processing element %d of %d) sent %d element(s) (e.g., %d) to rank %d. Kept %zu elements.\n",
                               my_rank, i + 1, N, send_size, to_send_next[0], my_rank + 1, local_sorted_data.size());
                        fflush(stdout);
                    }
                }
            }
        }

        // After all data elements have been fed, send an end-of-stream signal
        // (a message with size 0) to the next process, if there is one.
        if (num_procs > 1) {
            int end_signal_size = 0; // Size 0 indicates end of stream
            MPI_Send(&end_signal_size, 1, MPI_INT, my_rank + 1, DATA_TAG, MPI_COMM_WORLD);
            if (verbose) {
                printf("Rank %d sent END signal to rank %d.\n", my_rank, my_rank + 1);
                fflush(stdout);
            }
        }
    } else { // Worker processes (rank > 0)
        bool stream_active = true;
        while (stream_active) {
            int incoming_size;
            // Receive the size of the incoming message from the previous process
            MPI_Recv(&incoming_size, 1, MPI_INT, my_rank - 1, DATA_TAG, MPI_COMM_WORLD, &status);

            if (incoming_size == 0) { // End-of-stream signal received
                stream_active = false; // Exit the loop
                if (verbose) {
                    printf("Rank %d received END signal from rank %d.\n", my_rank, my_rank - 1);
                    fflush(stdout);
                }
                // Forward the end-of-stream signal to the next process, if not the last.
                if (my_rank < num_procs - 1) {
                    MPI_Send(&incoming_size, 1, MPI_INT, my_rank + 1, DATA_TAG, MPI_COMM_WORLD);
                     if (verbose) {
                        printf("Rank %d forwarded END signal to rank %d.\n", my_rank, my_rank + 1);
                        fflush(stdout);
                    }
                }
            } else { // Actual data chunk received
                std::vector<int> received_chunk(incoming_size);
                // Receive the actual data chunk
                MPI_Recv(received_chunk.data(), incoming_size, MPI_INT, my_rank - 1, DATA_TAG, MPI_COMM_WORLD, &status);
                if (verbose > 1 && !received_chunk.empty()) {
                    printf("Rank %d received %d element(s) (e.g., %d) from rank %d.\n",
                           my_rank, incoming_size, received_chunk[0], my_rank - 1);
                    fflush(stdout);
                }

                std::vector<int> to_send_next;
                // Process the received chunk: merge, sort, and split if necessary.
                // The nominal_capacity_per_proc is passed; merge_and_split correctly
                // handles the logic for the last process (which doesn't split).
                merge_and_split(local_sorted_data, received_chunk, nominal_capacity_per_proc, my_rank, num_procs, to_send_next);

                // If this process is not the last and has data to send forward
                if (my_rank < num_procs - 1 && !to_send_next.empty()) {
                    int send_size = to_send_next.size();
                    MPI_Send(&send_size, 1, MPI_INT, my_rank + 1, DATA_TAG, MPI_COMM_WORLD);
                    MPI_Send(to_send_next.data(), send_size, MPI_INT, my_rank + 1, DATA_TAG, MPI_COMM_WORLD);
                    if (verbose > 1 && !to_send_next.empty()) {
                         printf("Rank %d sent %d element(s) (e.g., %d) to rank %d. Kept %zu elements.\n",
                               my_rank, send_size, to_send_next[0], my_rank + 1, local_sorted_data.size());
                        fflush(stdout);
                    }
                }
            }
        }
    }

    // Synchronize all processes before starting the final collection phase
    MPI_Barrier(MPI_COMM_WORLD);

    // Collection phase: Gather all sorted segments at rank 0
    int my_local_data_size = local_sorted_data.size();
    std::vector<int> gather_recv_counts; // For rank 0: stores sizes from each process
    std::vector<int> gather_displacements; // For rank 0: stores displacements for Gatherv
    std::vector<int> final_sorted_array;   // For rank 0: the final assembled sorted array

    if (my_rank == 0) {
        gather_recv_counts.resize(num_procs);
    }

    // Step 1: Gather the size of each process's local_sorted_data at rank 0
    MPI_Gather(&my_local_data_size, 1, MPI_INT,       // Send buffer (size)
               gather_recv_counts.data(), 1, MPI_INT, // Receive buffer (array of sizes)
               0, MPI_COMM_WORLD);                    // Root process

    // Step 2: Rank 0 prepares for Gatherv and then collects all data
    if (my_rank == 0) {
        if (verbose) {
            printf("Rank 0: Received data sizes from all processes for Gatherv: ");
            for(int i=0; i < num_procs; ++i) printf("P%d=%d ", i, gather_recv_counts[i]);
            printf("\n");
            fflush(stdout);
        }

        gather_displacements.resize(num_procs);
        gather_displacements[0] = 0; // Displacement for rank 0's data is 0
        int total_elements_gathered = gather_recv_counts[0];

        for (int i = 1; i < num_procs; ++i) {
            gather_displacements[i] = gather_displacements[i-1] + gather_recv_counts[i-1];
            total_elements_gathered += gather_recv_counts[i];
        }
        
        // Validate if the total number of elements gathered matches the initial N
        // (Except if N was 0, in which case total_elements_gathered should also be 0)
        if (N == 0 && total_elements_gathered == 0) {
            final_sorted_array.resize(0); // Correct for N=0
        } else if (total_elements_gathered != N) {
            fprintf(stderr, "Rank 0 Error: Total elements gathered (%d) does not match N (%d).\n", total_elements_gathered, N);
            // This indicates a logic error in distribution or capacity management.
            // Resize to actual gathered size for inspection, but this is an error state.
            final_sorted_array.resize(total_elements_gathered);
        } else {
            final_sorted_array.resize(N); // Expected size
        }

        // Perform the Gatherv operation
        MPI_Gatherv(local_sorted_data.data(), my_local_data_size, MPI_INT, // Send part (for rank 0)
                    final_sorted_array.data(), gather_recv_counts.data(),  // Receive buffer and counts
                    gather_displacements.data(), MPI_INT,                  // Displacements
                    0, MPI_COMM_WORLD);                                    // Root

        if (verbose) {
            printf("Rank 0: Final sorted array (size %zu): ", final_sorted_array.size());
            // Print array contents if it's short or verbosity is high
            if (final_sorted_array.size() <= 50 || verbose > 1) {
                 for(size_t k=0; k < final_sorted_array.size(); ++k) printf("%d ", final_sorted_array[k]);
            } else {
                printf("[output truncated due to length; first few: ");
                for(size_t k=0; k < std::min((size_t)10, final_sorted_array.size()); ++k) printf("%d ", final_sorted_array[k]);
                if(final_sorted_array.size() > 10) printf("...");
                printf("]");
            }
            printf("\n");
            fflush(stdout);
        }

        // Verification of the sorted array
        bool sorted_correctly = true;
        if (final_sorted_array.size() != static_cast<size_t>(N) && N > 0) {
             sorted_correctly = false; 
             fprintf(stderr, "Rank 0 Error: Final array size %zu, expected %d.\n", final_sorted_array.size(), N);
        } else if (N > 0) { // Only verify order if N > 0
            for (size_t k = 0; k < final_sorted_array.size() - 1; ++k) {
                if (final_sorted_array[k] > final_sorted_array[k+1]) {
                    sorted_correctly = false;
                    fprintf(stderr, "Rank 0 Error: Sort verification failed at index %zu (%d > %d).\n", k, final_sorted_array[k], final_sorted_array[k+1]);
                    break;
                }
            }
        } else if (N == 0 && final_sorted_array.empty()) {
            sorted_correctly = true; // Empty array is considered sorted
        } else if (N == 0 && !final_sorted_array.empty()){
            sorted_correctly = false; // Should be empty if N=0
            fprintf(stderr, "Rank 0 Error: N is 0, but final array is not empty (size %zu).\n", final_sorted_array.size());
        }
        
        if (sorted_correctly) {
            // printf("Rank 0: Array sorted correctly.\n");

            float end_time = MPI_Wtime();
            printf("%d %f\n", num_procs, end_time - start_time);

        } else {
            printf("Rank 0: Array sorting FAILED.\n");
        }
        fflush(stdout);

    } else { // Other ranks (non-root) participate in MPI_Gatherv by sending their data
        MPI_Gatherv(local_sorted_data.data(), my_local_data_size, MPI_INT,
                    NULL, NULL, NULL, // Receive parameters are not used by non-root processes
                    MPI_INT, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}