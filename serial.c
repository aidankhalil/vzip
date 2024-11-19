/*OS Project 2
Worked on by:

Jose Jimenez
Aidan Khalil
Jayson Jensen
*/


#include <dirent.h> 
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>
#include <time.h>

#define BUFFER_SIZE 1048576 // 1MB
#define MAX_THREADS 20

int total_in = 0, total_out = 0;
pthread_mutex_t total_in_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t total_out_mutex = PTHREAD_MUTEX_INITIALIZER;


// Structure to hold arguments for each thread
typedef struct {
    char* filename;  // Filename of the file to be compressed
    char* directory; // Directory containing the file
} ThreadArgs;


/* Function to compress a file, it opens up the file path and compresses each individual ppm file and saves it to the root directory
of the source code*/
void* compress_file(void* arg) {
    ThreadArgs* args = (ThreadArgs*)arg; // Cast the argument to ThreadArgs structure
    char full_path[strlen(args->directory) + strlen(args->filename) + 2]; // Create full path to the file
    sprintf(full_path, "%s/%s", args->directory, args->filename);

    unsigned char buffer_in[BUFFER_SIZE];
    unsigned char buffer_out[BUFFER_SIZE];

    // load file
    FILE* f_in = fopen(full_path, "r"); // Open the file for reading
    assert(f_in != NULL);                // Ensure file is opened successfully
    int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in); // Read file contents into buffer
    fclose(f_in);

    // Lock the mutex before updating total_in
    pthread_mutex_lock(&total_in_mutex);
    total_in += nbytes; // Update total_in
    pthread_mutex_unlock(&total_in_mutex);


    // zip file
    z_stream strm;
    int ret = deflateInit(&strm, 9); // Initialize zlib stream for compression
    assert(ret == Z_OK);             // Ensure zlib initialization succeeds
    strm.avail_in = nbytes;           // Set available input bytes
    strm.next_in = buffer_in;         // Set input buffer
    strm.avail_out = BUFFER_SIZE;     // Set available output bytes
    strm.next_out = buffer_out;       // Set output buffer

    ret = deflate(&strm, Z_FINISH);   // Perform compression
    assert(ret == Z_STREAM_END);      // Ensure compression completes successfully

    // dump zipped file
    char zip_filename[strlen(args->filename) + 5]; // ".zzip\0"
    sprintf(zip_filename, "%s.zzip", args->filename); // Create name for zipped file
    FILE* f_out = fopen(zip_filename, "w"); // Open zipped file for writing
    assert(f_out != NULL);            // Ensure zipped file is opened successfully
    int nbytes_zipped = BUFFER_SIZE - strm.avail_out; // Calculate size of zipped data
    fwrite(&nbytes_zipped, sizeof(int), 1, f_out); // Write size of zipped data
    fwrite(buffer_out, sizeof(unsigned char), nbytes_zipped, f_out); // Write zipped data
    fclose(f_out); // Close zipped file

    // Lock the mutex before updating total_out
    pthread_mutex_lock(&total_out_mutex);
    total_out += nbytes_zipped; // Update total_out
    pthread_mutex_unlock(&total_out_mutex);
    
    free(args->filename); // Free allocated memory for filename
    free(args);           // Free allocated memory for arguments

    return NULL;
}

// Comparison function for qsort
int cmp(const void* a, const void* b) {
    return strcmp(*(char**)a, *(char**)b);
}

int main(int argc, char** argv) {
    // time computation header
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    // end of time computation header

    // do not modify the main function before this point!

    assert(argc == 2);

    DIR* d;
    struct dirent* dir;
    char** files = NULL;
    int nfiles = 0;

    d = opendir(argv[1]);
    if (d == NULL) {
        printf("An error has occurred\n");
        return 0;
    }

    // create sorted list of PPM files
    while ((dir = readdir(d)) != NULL) {
        files = realloc(files, (nfiles + 1) * sizeof(char*));
        assert(files != NULL);

        int len = strlen(dir->d_name);
        if (len > 4 && strcmp(dir->d_name + len - 4, ".ppm") == 0) {
            files[nfiles] = strdup(dir->d_name);
            assert(files[nfiles] != NULL);
            nfiles++;
        }
    }
    closedir(d);
    qsort(files, nfiles, sizeof(char*), cmp);

    // create a single zipped package with all PPM files in lexicographical order
    pthread_t *threads = malloc(nfiles * sizeof(pthread_t));
    if (threads == NULL) {
        perror("Failed to allocate memory for threads");
        exit(EXIT_FAILURE);
    }

    int active_threads = 0; // Track the number of active threads

    /*This loop will create the threads and its args and make sure that there is no more than 20 threads at the same time
    It does this by simply iterating through the number of files and creating a thread and keeping track of the individual threads
    If it reaches the maximum allowed it will wait until a thread is done and join and decrement the thread variable*/
    for (int i = 0; i < nfiles; i++) {
        ThreadArgs* args = (ThreadArgs*)malloc(sizeof(ThreadArgs));//create thread args
        if (args == NULL) {
            perror("Failed to allocate memory for ThreadArgs");
            exit(EXIT_FAILURE);
        }
        args->filename = strdup(files[i]);
        if (args->filename == NULL) {
            perror("Failed to allocate memory for filename");
            exit(EXIT_FAILURE);
        }
        args->directory = argv[1];

        int ret = pthread_create(&threads[i], NULL, compress_file, args);//create threads
        if (ret != 0) {
            perror("pthread_create failed");
            exit(EXIT_FAILURE);
        }

        active_threads++; // Increment the number of active threads

        if (active_threads >= MAX_THREADS) {
            // If maximum active threads reached, wait for some threads to finish
            for (int j = i - MAX_THREADS + 1; j <= i; j++) {
                pthread_join(threads[j], NULL);
                active_threads--;
            }
        }
    }

    // Join remaining threads
    for (int i = nfiles - active_threads; i < nfiles; i++) {
        pthread_join(threads[i], NULL);
    }

    free(threads);


    // Now, append all zzip files into "video.vzip"
    FILE* f_vzip = fopen("video.vzip", "wb"); // Open in binary mode
    assert(f_vzip != NULL);

    for (int i = 0; i < nfiles; i++) {
        char zip_filename[strlen(files[i]) + 6]; // ".zzip\0"
        sprintf(zip_filename, "%s.zzip", files[i]);
        FILE* f_zip = fopen(zip_filename, "rb"); // Open in binary mode
        assert(f_zip != NULL);

        int nbytes;
        while (fread(&nbytes, sizeof(int), 1, f_zip) == 1) {
            unsigned char buffer[BUFFER_SIZE];
            size_t bytes_read = fread(buffer, sizeof(unsigned char), nbytes, f_zip);
            assert(bytes_read == nbytes);
            fwrite(&nbytes, sizeof(int), 1, f_vzip); // Write uncompressed size
            fwrite(buffer, sizeof(unsigned char), nbytes, f_vzip);
        }

        fclose(f_zip);
        remove(zip_filename); // Remove the temporary zzip file
    }

    fclose(f_vzip);

	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

    // release list of files
    for (int i = 0; i < nfiles; i++)
        free(files[i]);
    free(files);

    // do not modify the main function after this point!

    // time computation footer
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Time: %.2f seconds\n", ((double)end.tv_sec + 1.0e-9 * end.tv_nsec) - ((double)start.tv_sec + 1.0e-9 * start.tv_nsec));
    // end of time computation footer

    return 0;
}
