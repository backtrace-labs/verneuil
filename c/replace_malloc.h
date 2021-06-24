#pragma once
#define _GNU_SOURCE

#include <stdlib.h>

#define malloc verneuil__zalloc

/**
 * This function forces sqlite3 to zero-fill all its allocations.
 */
static inline void *
verneuil__zalloc(size_t num)
{

        return calloc(num, 1);
}
