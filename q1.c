/**
 * q1.c
 * gcc -O3 -fopenmp -std=c99 q1.c -o q1
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

#include <omp.h>

// Value for the predicate to pass.
#define PASS 100


int NUM_PARALLEL_THREADS = 64;
int NUM_COPIES = 64;

// A bucket entry.
struct q1_entry {
    int32_t sum_qty;
    int32_t sum_base_price;
    int32_t sum_disc_price;
    int32_t sum_charge;
    int32_t sum_discount;
    int32_t count;

    // Pad to 32 bytes.
    int8_t _pad[8];
}; // __attribute__ ((aligned (256)))

// The generated input data.
struct gen_data {
    // Number of lineitems in the table.
    int32_t num_items;
    // Number of num_buckets/size of the hash table.
    int32_t num_buckets;
    // Probability that the branch in the query will be taken.
    float prob;
    // The input data.
    struct lineitem *items;
    struct q1_entry *buckets;
};

// An input data item represented as in a row format.
struct lineitem {
    // The Q1 bucket this lineitem is clustered in.
    int32_t bucket;

    // The branch condition (either PASS or FAIL).
    int32_t shipdate;

    // Various fields used in the query.
    int32_t quantity;
    int32_t extendedprice;
    int32_t discount;
    int32_t tax;

    // So the structure is 1/2 a cache line exactly.
    long _pad;
};

struct thread_data {
  struct gen_data *gd;
  struct q1_entry *buckets;
  int use_atomic;
  int tid;
};

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

    unsigned start = (td->gd->num_items / NUM_PARALLEL_THREADS) * td->tid;
    unsigned end = start + (td->gd->num_items / NUM_PARALLEL_THREADS);
    if (end > td->gd->num_items || td->tid == NUM_PARALLEL_THREADS - 1) {
        end = td->gd->num_items;
    }

    for (int i = start; i < end; i++) {
        struct lineitem *item = &td->gd->items[i];
        if (item->shipdate == PASS) {
            int bucket = item->bucket;
            struct q1_entry *e = &td->buckets[bucket];
            if (!td->use_atomic) {
              e->sum_qty += item->quantity;
              e->sum_base_price += item->extendedprice;
              e->sum_base_price += (item->extendedprice * item->discount);
              e->sum_charge +=
                  (item->extendedprice + item->discount) * (1 + item->tax);
              e->sum_discount += item->discount;
              e->count++;
            } else {
#pragma omp atomic
              e->sum_qty += item->quantity;
#pragma omp atomic
              e->sum_base_price += item->extendedprice;
#pragma omp atomic
              e->sum_disc_price += (item->extendedprice + item->discount);
#pragma omp atomic
              e->sum_charge +=
                (item->extendedprice + item->discount) * (1 + item->tax);
#pragma omp atomic
              e->sum_discount += item->discount;
#pragma omp atomic
              e->count++;
            }
        }
    }

    return NULL;
}

/** Runs a the query with thread-local hash tables which are merged at the end.
 *
 * @param d the input data.
 */
void run_query(struct gen_data *d, struct q1_entry *buckets) {
    struct timeval start, end, diff;

    gettimeofday(&start, 0);
    pthread_t threads[NUM_PARALLEL_THREADS];
    struct thread_data data[NUM_PARALLEL_THREADS];
    int threads_per_copy = NUM_PARALLEL_THREADS / NUM_COPIES;
    for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {
      data[i] = (struct thread_data) { d, buckets + (d->num_buckets * (i / threads_per_copy)), threads_per_copy > 1, i };
      pthread_create(threads + i, NULL, &run_helper, (void *)(data + i));
    }

    for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {
      pthread_join(threads[i], NULL);
    }
    gettimeofday(&end, 0);
    timersub(&end, &start, &diff);
    printf("main-time: %ld.%06ld\n",
            (long) diff.tv_sec, (long) diff.tv_usec);
    
    gettimeofday(&start, 0);
    if (NUM_COPIES > 1) {
      // Aggregate the values.
      for (int i = 0; i < NUM_COPIES; i++) {
          for (int j = 0; j < d->num_buckets; j++) {
              int b = i * d->num_buckets + j;
              d->buckets[j].sum_qty += buckets[b].sum_qty;
              d->buckets[j].sum_base_price += buckets[b].sum_base_price;
              d->buckets[j].sum_disc_price += buckets[b].sum_disc_price;
              d->buckets[j].sum_charge += buckets[b].sum_charge;
              d->buckets[j].sum_discount += buckets[b].sum_discount;
              d->buckets[j].count += buckets[b].count;
          }
      }
    } else {
      memcpy(d->buckets, buckets, sizeof(struct q1_entry) * d->num_buckets);
    }
    gettimeofday(&end, 0);
    timersub(&end, &start, &diff);
    printf("agg-time: %ld.%06ld\n",
            (long) diff.tv_sec, (long) diff.tv_usec);

}

/** Generates input data.
 *
 * @param num_items the number of line items.
 * @param num_buckets the number of buckets we hash into.
 * @param prob the selectivity of the branch.
 * @return the generated data in a structure.
 */
struct gen_data generate_data(int num_items, int num_buckets, float prob) {
    struct gen_data d;

    d.num_items = num_items;
    d.num_buckets = num_buckets;
    d.prob = prob;

    d.items = (struct lineitem *)malloc(sizeof(struct lineitem) * num_items);
    d.buckets = (struct q1_entry *)malloc(sizeof(struct q1_entry) * num_buckets);

    int pass_thres = (int)(prob * 100.0);
    for (int i = 0; i < d.num_items; i++) {
        struct lineitem *item = &d.items[i];
        if (random() % 100 <= pass_thres) {
            item->shipdate = PASS;
        } else {
            item->shipdate = 0;
        }

        int seed = random();

        // Random values.
        item->quantity = seed;
        item->extendedprice = seed + 1;
        item->discount = seed + 2;
        item->tax = seed + 3;

        item->bucket = random() % num_buckets;
    }

    // 0 out the num_buckets.
    memset(d.buckets, 0, sizeof(struct q1_entry) * d.num_buckets);
    return d;
}

double gb_s(int num_items, int num_its, struct timeval diff) {
  printf("total-time: %ld.%06ld\n",
          (long) diff.tv_sec, (long) diff.tv_usec);
  double secs = diff.tv_sec + diff.tv_usec / ((double)1e6) - 0.01 * num_its; // correction for thread startup
  return ((uint64_t)num_items) * num_its * sizeof(struct lineitem) / ((double)1e9) / secs;
}

int main(int argc, char **argv) {
    // Number of bucket entries (default = 6, same as TPC-H Q1)
    int num_buckets = 6;
    // Number of elements in array (should be >> cache size);
    int num_items = 1E8/sizeof(int);
    // Approx. PASS probability.
    float prob = 0.01;

    int ch;
    while ((ch = getopt(argc, argv, "b:n:p:t:c:")) != -1) {
        switch (ch) {
            case 'b':
                num_buckets = atoi(optarg);
                break;
            case 'n':
                num_items = atoi(optarg);
                break;
            case 'p':
                prob = atof(optarg);
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
    assert(prob >= 0.0 && prob <= 1.0);
    assert(num_buckets % 2 == 0);
    assert(NUM_PARALLEL_THREADS > 0);
    assert(NUM_COPIES > 0);
    assert(NUM_PARALLEL_THREADS % NUM_COPIES == 0);

    omp_set_num_threads(NUM_PARALLEL_THREADS);
    printf("n=%d, b=%d, p=%f, t=%d, c=%d\n\n", num_items, num_buckets, prob, NUM_PARALLEL_THREADS, NUM_COPIES);

    struct gen_data d = generate_data(num_items, num_buckets, prob);
    long sum;
    struct timeval start, end, diff, total;
    timerclear(&total);

    struct q1_entry *buckets = (struct q1_entry *)malloc(
            sizeof(struct q1_entry) * d.num_buckets * NUM_COPIES);
    int ITS = 10;
    for (int i = 0; i < ITS; i++) {
      // Reset the buckets.
      memset(d.buckets, 0, sizeof(struct q1_entry) * d.num_buckets);
      memset(buckets, 0, sizeof(struct q1_entry) * d.num_buckets * NUM_COPIES);

      gettimeofday(&start, 0);
      run_query(&d, buckets);
      gettimeofday(&end, 0);

      timersub(&end, &start, &diff);
      timeradd(&total, &diff, &total);
      printf("result: %d %d\n\n",
              d.buckets[0].count, d.buckets[0].sum_charge);
    }

    printf("%0.2f GB/s\n", gb_s(num_items, ITS, total));
    free(buckets);

    return 0;
}
