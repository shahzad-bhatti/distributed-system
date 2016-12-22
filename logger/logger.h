#pragma once

#include <string>
#include <sstream>
#include <mutex>
#include <memory>
#include <fstream>

using namespace std;

// log message levels
enum level	{ DEBUG2, DEBUG, INFO, WARN, ERROR };
class logger;


class logstream : public ostringstream {
public:
	logstream(logger& oLogger, level nLevel);
	logstream(const logstream& ls);
	~logstream();

private:
	logger& m_oLogger;
	level m_nLevel;
};


class logger {
public:
    logger(string filename);
	virtual ~logger();

	void log(level nLevel, string oMessage);

	logstream operator()();
	logstream operator()(level nLevel);
    
    void setLevel(level nLevel);

private:
	const tm* getLocalTime();
	mutex m_oMutex;
	ofstream m_oFile;
    level sLevel;
	tm m_oLocalTime;
};

