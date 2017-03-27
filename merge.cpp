// clang++-3.6 -O3 -std=c++11 merge.cpp
#include <sys/time.h>
#include <stdlib.h>
#include <algorithm>

using namespace std;

int main(int argc, char **argv) {
  int LEN = atoi(argv[1]);
  
  int *one = new int[LEN + 1];
  int *two = new int[LEN + 1];
  for (int i = 0; i < LEN; i++) {
    one[i] = rand() % 1000000;
    two[i] = rand() % 1000000;
  }
  one[LEN] = 1000000;
  two[LEN] = 1000000;

  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  std::sort(one, one + LEN); 
  std::sort(two, two + LEN); 
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("Sort: %ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);

  int *out = new int[LEN * 2];
  gettimeofday(&start, 0);
  int one_ptr = 0;
  int two_ptr = 0;
  while (one_ptr < LEN || two_ptr < LEN) {
    if (one[one_ptr] < two[two_ptr]) {
      out[one_ptr + two_ptr] = one[one_ptr];
      one_ptr++;
    } else {
      out[one_ptr + two_ptr] = two[two_ptr];
      two_ptr++;
    }
  }
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("Merge: %ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);

  return 0;
}
