#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>
#include <assert.h>
#include <pthread.h>

#include "float_vec.h"
#include "utils.h"

pthread_mutex_t mutex;
pthread_cond_t  condv;
int FLAG = 0;

typedef struct thread_args{
        floats* arg_input;
        const char* output_str;
        int thread_count;
        floats* arg_samps;
        long* arg_sizes;
        int thread_index;
        floats* ml;

    } thread_args;

int new_strcmp (const void* a, const void* b) {
    int t = (*(int*)a - *(int*)b);
   return t;
}

void
qsort_floats(floats* xs)
{
    qsort(xs->data, xs->size, sizeof(float), new_strcmp);
}

floats*
sample(floats* input, int P)
{
    int index = (3 * (P - 1));
    floats* samps = make_floats(0);
    float temp[index];

    // Add 0 to the beginning
    floats_push(samps, 0);

    // randomly select 3 * (P - 1) elements from the array
    srand(time(NULL));
    for (int i = 0; i < index; i++) {
        int r = rand() % input->size;
        temp[i] = input->data[r];
    }

    // sort randomly selected samples
    qsort(temp, index, sizeof(float), new_strcmp);

    // Take the median of each group of three in the sorted array, producing an array (samples) of (P-1) items
    int i = 0;
    while(samps->size < P) {
        floats_push(samps, temp[i + 1]);
        i+=2;
    }

    // Add INFINITY to the end
    floats_push(samps, INFINITY);

    return samps;
}

void
sort_worker(int pnum, floats* input, const char* output, int P, floats* samps, long* sizes, floats* master_list)
{

    // make local array (bucket)
    floats* xs = make_floats(0);

    // Find all input values that are between pnum and pnum + 1
    for(int i = 0; i < input->size; i++) {
        if ((input->data[i] > samps->data[pnum]) && (input->data[i] <= samps->data[pnum + 1])) {
            floats_push(xs, input->data[i]);
        }
    }

    // Sort the bucket
    qsort_floats(xs);

    printf("%d: start %.04f, count %ld\n", pnum, samps->data[pnum], xs->size);

    pthread_mutex_lock(&mutex);
    while(FLAG != pnum) {
        pthread_cond_wait(&condv, &mutex);
    }

    for(int i = 0; i < xs->size; i++) {
        floats_push(master_list, xs->data[i]);
    }

    FLAG++;
    pthread_cond_broadcast(&condv);
    pthread_mutex_unlock(&mutex);

    free_floats(xs);
}

void*
independant_thread(void* arg)
{
    thread_args a = *((thread_args*) arg);
    free(arg);
    sort_worker(a.thread_index, a.arg_input, a.output_str, a.thread_count, a.arg_samps, a.arg_sizes, a.ml);

    return 0;
}

void
run_sort_workers(floats* input, const char* output, int P, floats* samps, long* sizes, floats* master_list)
{
    pthread_t threads[P];
    pthread_mutex_init(&mutex, 0);
    pthread_cond_init(&condv, 0);

    // Spawn P threads running sort_worker
    for (int i = 0; i < P; i++) {

        thread_args *arg = malloc(sizeof *arg); 
        arg->arg_input = input;
        arg->output_str = output;
        arg->thread_count = P;
        arg->arg_samps = samps;
        arg->arg_sizes = sizes;
        arg->thread_index = i;
        arg->ml = master_list;

        int rv = pthread_create(&(threads[i]), 0, independant_thread, arg);
        assert(rv == 0);
    }

    for (int i = 0; i < P; i++) {
        int rv = pthread_join(threads[i], 0);
        assert(rv == 0);
    }
}

void
sample_sort(floats* input, const char* output, int P, long* sizes, floats* master_list)
{
    floats* samps = sample(input, P);
    run_sort_workers(input, output, P, samps, sizes, master_list);
    free_floats(samps);
}

int
main(int argc, char* argv[])
{
    alarm(120);
    floats* master_list = make_floats(0);

    if (argc != 4) {
        printf("Usage:\n");
        printf("\t%s P input.dat output.dat\n", argv[0]);
        return 1;
    }

    const int P = atoi(argv[1]);
    const char* iname = argv[2];
    const char* oname = argv[3];

    seed_rng();

    floats* input = make_floats(0);

    int ifd = open(iname, O_RDONLY);
    check_rv(ifd);
	long c = 0;
    read(ifd, &c, sizeof(long));
    int  rv;

    lseek(ifd, sizeof(long), SEEK_SET);
    for (long i = 0; i < c; i++) {
        float n = 0.0f;
        read(ifd, &n, sizeof(float));
        floats_push(input, n);
    }

    rv = close(ifd);
    check_rv(rv);

    int ofd = open(oname, O_CREAT|O_TRUNC|O_WRONLY);
    check_rv(ofd);

    int size = lseek(ifd, 0, SEEK_END);
    ftruncate(ofd, size);

    write(ofd, &c, sizeof(long));

    rv = close(ofd);
    check_rv(rv);

    long* sizes = malloc(P * sizeof(long));
    sample_sort(input, oname, P, sizes, master_list);

    int lfd = open(oname, O_WRONLY);

    lseek(lfd, sizeof(long), SEEK_SET);
    for(int i = 0; i < master_list->size; i++) {
        write(lfd, &master_list->data[i], sizeof(float));
    }

    rv = close(ofd);
    check_rv(rv);

    free(sizes);

    free_floats(input);

    return 0;
}

