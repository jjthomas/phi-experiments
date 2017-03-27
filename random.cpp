#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <vector>
#include <algorithm>


using namespace std;

struct node {
  int data;
  node *next;
};

int main(int argc, char **argv) {
  int SIZE = 100000000;
  int ITS = 1;
  vector<int> indices;
  node *nodes = new node[SIZE];
  for (int i = 0; i < SIZE; i++) {
    indices.push_back(i);
    nodes[i].data = i;
  }
  random_shuffle(indices.begin(), indices.end());
  int prev = indices[0];
  nodes[prev].next = NULL;
  for (int i = 1; i < SIZE; i++) {
    nodes[prev].next = &nodes[indices[i]]; 
    prev = indices[i];
    nodes[prev].next = NULL;
  }

  struct timeval start, end, diff;
  int sum = 0;
  for (int i = 0; i < ITS; i++) { 
    sum = 0;
    gettimeofday(&start, 0);
    for (int j = 0; j < SIZE; j++) {
      sum += nodes[j].data;
    }
    gettimeofday(&end, 0);
    timersub(&end, &start, &diff);
    printf("SEQ %d: %ld.%06ld\n", sum, (long)diff.tv_sec, (long)diff.tv_usec);

    sum = 0;
    gettimeofday(&start, 0);
    for (int j = 0; j < SIZE; j++) {
      sum += nodes[indices[j]].data;
    }
    gettimeofday(&end, 0);
    timersub(&end, &start, &diff);
    printf("RAND %d: %ld.%06ld\n", sum, (long)diff.tv_sec, (long)diff.tv_usec);

    sum = 0;
    gettimeofday(&start, 0);
    node *cur = &nodes[indices[0]];
    while (cur != NULL) {
      sum += cur->data;
      cur = cur->next;
    }
    gettimeofday(&end, 0);
    timersub(&end, &start, &diff);
    printf("LINKED %d: %ld.%06ld\n", sum, (long)diff.tv_sec, (long)diff.tv_usec);
  }
  return 0;
}
