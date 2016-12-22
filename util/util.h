/*
 * @file util.h
 * @author Shahzad Bhatti
 * @date Sep 29, 2016
 *
 */
#pragma once

#include <cstdint>
#include <netinet/in.h>
#include <sys/time.h>
#include <string>
#include <memory>
#include <fstream>

using namespace std;

uint64_t htonll(uint64_t host_longlong);

uint64_t ntohll(uint64_t host_longlong);

uint64_t timeNow();

bool isPrefix(const string& prefix, const string& str);

bool fileExists(const std::string& name);


/**
 * Return a hash for some given string
 */
unsigned hash_str(const char* s);

/**
 * partition strings into two parts based on first occurrence of c
 */
void partition_string(string str, char c, string& str1, string& str2);

bool replace(std::string& str, const std::string& from, const std::string& to);

/*
 * runs popen and returns result to a string
 *
 */
string exec(const char* cmd);

/*
 *
 * parse file name from map_5_apple to map_apple
 *
 */
//string parseFileName(string fileName);

/*
 *
 * get key from intermediat filename e.g apple from map_5_apple
 *
 */
string getKey(string fileName);

//string getKeyForJuice(string fileName);

