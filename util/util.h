/*
 * @file util.h
 * @author Shahzad Bhatti
 * @date Sep 29, 2016
 *
 */
#pragma once

#include <cstdint>


uint64_t htonll(uint64_t host_longlong);

uint64_t ntohll(uint64_t host_longlong);

uint64_t timeNow();

