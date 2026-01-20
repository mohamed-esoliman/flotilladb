#pragma once

#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <string>

namespace flotilla {

enum class LogLevel { kDebug = 0, kInfo, kWarn, kError };

class Logger {
 public:
  static LogLevel& MinLevel() {
    static LogLevel level = LogLevel::kInfo;
    return level;
  }

  static std::string& Prefix() {
    static std::string prefix;
    return prefix;
  }

  static void Log(LogLevel level, const char* fmt, ...) {
    if (level < MinLevel()) return;
    const char* names[] = {"DEBUG", "INFO", "WARN", "ERROR"};
    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    tm tm_buf;
    localtime_r(&ts.tv_sec, &tm_buf);
    char when[32];
    strftime(when, sizeof(when), "%H:%M:%S", &tm_buf);
    fprintf(stderr, "%s.%03ld %s %s%s\n", when, ts.tv_nsec / 1000000,
            names[static_cast<int>(level)], Prefix().c_str(), msg);
  }
};

#define FLOG_DEBUG(...) ::flotilla::Logger::Log(::flotilla::LogLevel::kDebug, __VA_ARGS__)
#define FLOG_INFO(...) ::flotilla::Logger::Log(::flotilla::LogLevel::kInfo, __VA_ARGS__)
#define FLOG_WARN(...) ::flotilla::Logger::Log(::flotilla::LogLevel::kWarn, __VA_ARGS__)
#define FLOG_ERROR(...) ::flotilla::Logger::Log(::flotilla::LogLevel::kError, __VA_ARGS__)

}  // namespace flotilla
