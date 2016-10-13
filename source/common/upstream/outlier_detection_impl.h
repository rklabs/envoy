#pragma once

#include "envoy/common/time.h"
#include "envoy/event/timer.h"
#include "envoy/runtime/runtime.h"
#include "envoy/upstream/outlier_detection.h"
#include "envoy/upstream/upstream.h"

#include "common/json/json_loader.h"

namespace Upstream {

/**
 * Null host sink implementation.
 */
class OutlierDetectorHostSinkNullImpl : public OutlierDetectorHostSink {
public:
  // Upstream::OutlierDetectorHostSink
  void putHttpResponseCode(uint64_t) override {}
  void putResponseTime(std::chrono::milliseconds) override {}
};

/**
 * Factory for creating a detector from a JSON configuration.
 */
class OutlierDetectorImplFactory {
public:
  static OutlierDetectorPtr createForCluster(Cluster& cluster, const Json::Object& cluster_config,
                                             Event::Dispatcher& dispatcher,
                                             Runtime::Loader& runtime, Stats::Store& stats);
};

class OutlierDetectorImpl;

/**
 * Implementation of OutlierDetectorHostSink for the generic detector.
 */
class OutlierDetectorHostSinkImpl : public OutlierDetectorHostSink {
public:
  OutlierDetectorHostSinkImpl(OutlierDetectorImpl& detector, HostPtr host)
      : detector_(detector), host_(host) {}

  void eject(SystemTime ejection_time);
  SystemTime ejectionTime() { return ejection_time_; }
  uint32_t numEjections() { return num_ejections_; }

  // Upstream::OutlierDetectorHostSink
  void putHttpResponseCode(uint64_t response_code) override;
  void putResponseTime(std::chrono::milliseconds) override {}

private:
  OutlierDetectorImpl& detector_;
  std::weak_ptr<Host> host_;
  std::atomic<uint32_t> consecutive_5xx_{0};
  SystemTime ejection_time_;
  uint32_t num_ejections_{};
};

/**
 * All outlier detection stats. @see stats_macros.h
 */
// clang-format off
#define ALL_OUTLIER_DETECTION_STATS(COUNTER, GAUGE)                                                \
  COUNTER(ejections_total)                                                                         \
  GAUGE  (ejections_active)                                                                        \
  COUNTER(ejections_overflow)                                                                      \
  COUNTER(ejections_consecutive_5xx)
// clang-format on

/**
 * Struct definition for all outlier detection stats. @see stats_macros.h
 */
struct OutlierDetectionStats {
  ALL_OUTLIER_DETECTION_STATS(GENERATE_COUNTER_STRUCT, GENERATE_GAUGE_STRUCT)
};

/**
 * An implementation of an outlier detector. In the future we may support multiple outlier detection
 * implementations with different configuration. For now, as we iterate everything is contained
 * within this implementation.
 */
class OutlierDetectorImpl : public OutlierDetector {
public:
  /**
   * FIXFIX
   */
  void onConsecutive5xx(HostPtr host);
  Runtime::Loader& runtime() { return runtime_; }

  // Upstream::OutlierDetector
  void addChangedStateCb(ChangeStateCb cb) override { callbacks_.push_back(cb); }

protected:
  OutlierDetectorImpl(Cluster& cluster, Event::Dispatcher& dispatcher, Runtime::Loader& runtime,
                      Stats::Store& stats, SystemTimeSource& time_source);

private:
  void addHostSink(HostPtr host);
  void armIntervalTimer();
  void checkHostForUneject(HostPtr host, OutlierDetectorHostSinkImpl* sink, SystemTime now);
  void ejectHost(HostPtr host);
  static OutlierDetectionStats generateStats(const std::string& name, Stats::Store& store);
  void onConsecutive5xxWorker(HostPtr host);
  void onIntervalTimer();
  void runCallbacks(HostPtr host);

  Event::Dispatcher& dispatcher_;
  Runtime::Loader& runtime_;
  SystemTimeSource& time_source_;
  OutlierDetectionStats stats_;
  Event::TimerPtr interval_timer_;
  std::list<ChangeStateCb> callbacks_;
  std::unordered_map<HostPtr, OutlierDetectorHostSinkImpl*> host_sinks_;
};

/**
 * FIXFIX
 */
class ProdOutlierDetectorImpl : public OutlierDetectorImpl, public SystemTimeSource {
public:
  ProdOutlierDetectorImpl(Cluster& cluster, Event::Dispatcher& dispatcher, Runtime::Loader& runtime,
                          Stats::Store& stats)
      : OutlierDetectorImpl(cluster, dispatcher, runtime, stats, *this) {}

  // SystemTimeSource
  SystemTime currentSystemTime() override { return std::chrono::system_clock::now(); }
};

} // Upstream
