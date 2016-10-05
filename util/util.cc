/*
 * @file util.cc
 * @author Shahzad Bhatti
 * @date Sep 29, 2016
 *
 */

#include <cstdint>
#include <netinet/in.h>
#include <sys/time.h>


uint64_t htonll(uint64_t host_longlong) {
    int x = 1;
    /* little endian */
    if(*(char *)&x == 1)
        return ((((uint64_t)htonl(host_longlong)) << 32) + htonl(host_longlong >> 32));
    /* big endian */
    else
        return host_longlong;
}
 
uint64_t ntohll(uint64_t host_longlong) {
    int x = 1;
    /* little endian */
    if(*(char *)&x == 1)
        return ((((uint64_t)ntohl(host_longlong)) << 32) + ntohl(host_longlong >> 32));
    /* big endian */
    else
        return host_longlong;
}

uint64_t timeNow() {
    struct timeval current;
    gettimeofday(&current, 0);
    return current.tv_sec * 1000 * 1000 + current.tv_usec;
}


