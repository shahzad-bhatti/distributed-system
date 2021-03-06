#include "logger.h"

#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>


// Convert date and time info from tm to a character string
// in format "YYYY-mm-DD HH:MM:SS" and send it to a stream
ostream& operator<< (ostream& stream, const tm* tm) {
//  had to muck around this section since GCC 4.8.1 did not implement put_time
//	return stream << put_time(tm, "%Y-%m-%d %H:%M:%S");
	return stream << 1900 + tm->tm_year << '-' <<
		setfill('0') << setw(2) << tm->tm_mon + 1 << '-'
		<< setfill('0') << setw(2) << tm->tm_mday << ' '
		<< setfill('0') << setw(2) << tm->tm_hour << ':'
		<< setfill('0') << setw(2) << tm->tm_min << ':'
		<< setfill('0') << setw(2) << tm->tm_sec;
}


logger::logger(string filename) {
	//m_oFile.open(filename, fstream::out | fstream::app | fstream::ate);
	m_oFile.open(filename, fstream::out);
    sLevel = INFO;
}


logger::~logger() {
	m_oFile.flush();
	m_oFile.close();
}


logstream logger::operator()() {
	return logstream(*this, INFO);
}


logstream logger::operator()(level nLevel) {
	return logstream(*this, nLevel);
}


const tm* logger::getLocalTime() {
	auto in_time_t = chrono::system_clock::to_time_t(chrono::system_clock::now());
	localtime_r(&in_time_t, &m_oLocalTime);
	return &m_oLocalTime;
}


void logger::setLevel(level lev) {
    sLevel = lev;
}


void logger::log(level nLevel, string oMessage) {
    if (nLevel < sLevel)
        return;
	const static char* levelStr[] = { "DEBUG2", "DEBUG", "INFO", "WARN", "ERROR" };

	m_oMutex.lock();
	m_oFile << '[' << getLocalTime() << ']'
		<< '[' << levelStr[nLevel] << "]\t"
		<< oMessage << endl;
	m_oMutex.unlock();
}


logstream::logstream(logger& oLogger, level nLevel) :
m_oLogger(oLogger), m_nLevel(nLevel) {
    
}


logstream::logstream(const logstream& ls) :
basic_ios<char>(), ostringstream(), m_oLogger(ls.m_oLogger), m_nLevel(ls.m_nLevel) {
}


logstream::~logstream() {
	m_oLogger.log(m_nLevel, str());
}

