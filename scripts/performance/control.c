/* SPDX-License-Identifier: Apache-2.0 */
/* Exact external-tool witnesses. These are not Magnetar client costs. */
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

__attribute__((noinline))
static void forced_copy(unsigned char *destination, const unsigned char *source) {
    memcpy(destination, source, 256);
}

int main(int argc, char **argv) {
    if (argc != 3) return 2;
    unsigned long count = strtoul(argv[2], NULL, 10);
    if (count == 0 || count > 100000) return 2;
    if (strcmp(argv[1], "invalid") == 0) return 7;
    if (strcmp(argv[1], "empty") == 0) return 0;
    if (strcmp(argv[1], "syscall") == 0) {
        for (unsigned long i = 0; i < count; i++) {
            if (syscall(SYS_getpid) <= 0) return 3;
        }
        return 0;
    }
    if (strcmp(argv[1], "allocation") == 0) {
        for (unsigned long i = 0; i < count; i++) {
            char *a = malloc(1024);
            char *b = calloc(1, 1024);
            if (!a || !b) return 3;
            char *grown = realloc(a, 2048);
            if (!grown) return 3;
            grown[2047] = b[0];
            free(grown);
            free(b);
        }
        return 0;
    }
    if (strcmp(argv[1], "copy") == 0) {
        unsigned char *a = malloc(256), *b = malloc(256);
        if (!a || !b) return 3;
        memset(a, 37, 256);
        for (unsigned long i = 0; i < count; i++) {
            forced_copy(b, a);
            if (b[255] != 37) return 3;
        }
        free(a);
        free(b);
        return 0;
    }
    return 2;
}
