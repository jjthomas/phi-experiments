/**
 * q1.c
 * gcc -O3 -fopenmp -lnuma -std=c99 count.c -o count 
 *
 * A test for varying parameters for queries similar to Q1.
 *
 */

#ifdef __linux__
#define _BSD_SOURCE 500
#define _POSIX_C_SOURCE 2
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>
#include <sched.h>
#include <numa.h>

#include <omp.h>

int NUM_PARALLEL_THREADS = 64;
int NUM_COPIES = 64;
int ITS = 10;

// The generated input data.
struct gen_data {
    // Number of lineitems in the table.
    int32_t num_items;
    // Number of num_buckets/size of the hash table.
    int32_t num_buckets;
    // The input data.
    uint32_t *items;
    uint32_t *buckets;
};

struct thread_data {
  struct gen_data *gd;
  uint32_t **copies;
  int use_atomic;
  int tid;
};

pthread_barrier_t bar;

/**
 * Runs a worker for the query with thread-local hash tables.
 */
void *run_helper(void *data) {
    struct thread_data *td = (struct thread_data *)data;

    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(td->tid, &set);
    if (sched_setaffinity(0, sizeof(set), &set) == -1) {
      printf("unable to set affinitiy for thread %d\n", td->tid);
    }

    int threads_per_copy = NUM_PARALLEL_THREADS / NUM_COPIES; 
    if (td->tid % threads_per_copy == 0) {
      td->copies[td->tid / threads_per_copy] = (uint32_t *)numa_alloc_onnode(sizeof(uint32_t) * td->gd->num_buckets,
	numa_node_of_cpu(td->tid) + 4);
    }
    pthread_barrier_wait(&bar);

    uint32_t *buckets = td->copies[td->tid / threads_per_copy];
    unsigned start = (td->gd->num_items / NUM_PARALLEL_THREADS) * td->tid;
    unsigned end = start + (td->gd->num_items / NUM_PARALLEL_THREADS);
    if (end > td->gd->num_items || td->tid == NUM_PARALLEL_THREADS - 1) {
      end = td->gd->num_items;
    }

    for (int it = 0; it < ITS; it++) {
      if (!td->use_atomic) {
        for (int i = start; i < end; i++) {
          uint32_t item = td->gd->items[i];
          buckets[item]++;
        }
      } else {
        for (int i = start; i < end; i++) {
          uint32_t item = td->gd->items[i];
#pragma omp atomic
          buckets[item]++;
        }
      }
      pthread_barrier_wait(&bar);
      // for agg to complete
      pthread_barrier_wait(&bar);
    }

    return NULL;
}

double gb_s(int num_items, struct timeval diff) {
  double secs = diff.tv_sec + diff.tv_usec / ((double)1e6);
  return ((uint64_t)num_items) * ITS * sizeof(uint32_t) / ((double)1e9) / secs;
}

/** Runs a the query with thread-local hash tables which are merged at the end.
 *
 * @param d the input data.
 */
void run_query(struct gen_data *d) {
    pthread_t threads[NUM_PARALLEL_THREADS];
    struct thread_data data[NUM_PARALLEL_THREADS];
    uint32_t *copies[NUM_COPIES];
    int threads_per_copy = NUM_PARALLEL_THREADS / NUM_COPIES;
    pthread_barrier_init(&bar, NULL, NUM_PARALLEL_THREADS + 1);

    for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {
      data[i] = (struct thread_data) { d, copies, threads_per_copy > 1, i };
      pthread_create(threads + i, NULL, &run_helper, (void *)(data + i));
    }

    // for init
    pthread_barrier_wait(&bar);

    struct timeval start, end, diff, total;
    timerclear(&total);

    for (int i = 0; i < ITS; i++) {
      gettimeofday(&start, 0);
      pthread_barrier_wait(&bar);
      if (NUM_COPIES > 1) {
        // Aggregate the values.
        for (int i = 0; i < NUM_COPIES; i++) {
            for (int j = 0; j < d->num_buckets; j++) {
              d->buckets[j] += copies[i][j];
            }
        }
      } else {
        memcpy(d->buckets, copies[0], sizeof(uint32_t) * d->num_buckets);
      }
      gettimeofday(&end, 0);
      timersub(&end, &start, &diff);
      timeradd(&total, &diff, &total);
      // agg complete
      pthread_barrier_wait(&bar);
    }

    printf("%0.2f GB/s\n", gb_s(d->num_items, total));

    for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {
      pthread_join(threads[i], NULL);
    }
}

/** Generates input data.
 *
 * @param num_items the number of line items.
 * @param num_buckets the number of buckets we hash into.
 * @param prob the selectivity of the branch.
 * @return the generated data in a structure.
 */
struct gen_data generate_data(int num_items, int num_buckets) {
    struct gen_data d;

    d.num_items = num_items;
    d.num_buckets = num_buckets;
    d.items = (uint32_t *)numa_alloc_onnode(sizeof(uint32_t) * num_items, 4);
    d.buckets = (uint32_t *)numa_alloc_onnode(sizeof(uint32_t) * num_buckets, 4);

    for (int i = 0; i < d.num_items; i++) {
      d.items[i] = random() % num_buckets;
    }

    // 0 out the num_buckets.
    memset(d.buckets, 0, sizeof(uint32_t) * d.num_buckets);
    return d;
}

int main(int argc, char **argv) {
    // Number of bucket entries (default = 6, same as TPC-H Q1)
    int num_buckets = 6;
    // Number of elements in array (should be >> cache size);
    int num_items = 1E8/sizeof(int);

    int ch;
    while ((ch = getopt(argc, argv, "b:n:t:c:")) != -1) {
        switch (ch) {
            case 'b':
                num_buckets = atoi(optarg);
                break;
            case 'n':
                num_items = atoi(optarg);
                break;
	    case 't':
                NUM_PARALLEL_THREADS = atoi(optarg);
                break;
	    case 'c':
                NUM_COPIES = atoi(optarg);
                break;
            case '?':
            default:
                fprintf(stderr, "invalid options");
                exit(1);
        }
    }

    // Check parameters.
    assert(num_buckets > 0);
    assert(num_items > 0);
    assert(num_buckets % 2 == 0);
    assert(NUM_PARALLEL_THREADS > 0);
    assert(NUM_COPIES > 0);
    assert(NUM_PARALLEL_THREADS % NUM_COPIES == 0);

    omp_set_num_threads(NUM_PARALLEL_THREADS);
    printf("n=%d, b=%d, t=%d, c=%d\n\n", num_items, num_buckets, NUM_PARALLEL_THREADS, NUM_COPIES);

    struct gen_data d = generate_data(num_items, num_buckets);
    run_query(&d);
    return 0;
}
