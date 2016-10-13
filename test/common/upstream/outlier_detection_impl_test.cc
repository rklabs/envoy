#include "common/upstream/outlier_detection_impl.h"
#include "common/upstream/upstream_impl.h"

#include "test/mocks/event/mocks.h"
#include "test/mocks/runtime/mocks.h"
#include "test/mocks/upstream/mocks.h"

using testing::_;
using testing::NiceMock;
using testing::Return;

namespace Upstream {

TEST(OutlierDetectorImplFactoryTest, NoDetector) {
  Json::StringLoader loader("{}");
  NiceMock<MockCluster> cluster;
  NiceMock<Event::MockDispatcher> dispatcher;
  NiceMock<Runtime::MockLoader> runtime;
  Stats::IsolatedStoreImpl stats_store;
  EXPECT_EQ(nullptr, OutlierDetectorImplFactory::createForCluster(cluster, loader, dispatcher,
                                                                  runtime, stats_store));
}

TEST(OutlierDetectorImplFactoryTest, Detector) {
  std::string json = R"EOF(
  {
    "outlier_detection": {}
  }
  )EOF";

  Json::StringLoader loader(json);
  NiceMock<MockCluster> cluster;
  NiceMock<Event::MockDispatcher> dispatcher;
  NiceMock<Runtime::MockLoader> runtime;
  Stats::IsolatedStoreImpl stats_store;
  EXPECT_NE(nullptr, OutlierDetectorImplFactory::createForCluster(cluster, loader, dispatcher,
                                                                  runtime, stats_store));
}

class TestOutlierDetectorImpl : public OutlierDetectorImpl, public SystemTimeSource {
public:
  TestOutlierDetectorImpl(Cluster& cluster, Event::Dispatcher& dispatcher, Runtime::Loader& runtime,
                          Stats::Store& stats)
      : OutlierDetectorImpl(cluster, dispatcher, runtime, stats, *this) {}

  // SystemTimeSource
  MOCK_METHOD0(currentSystemTime, SystemTime());
};

class CallbackChecker {
public:
  MOCK_METHOD1(check, void(HostPtr host));
};

class OutlierDetectorImplTest : public testing::Test {
public:
  OutlierDetectorImplTest() {
    ON_CALL(runtime_.snapshot_, featureEnabled("outlier_detection.enforcing", 100))
        .WillByDefault(Return(true));
  }

  NiceMock<MockCluster> cluster_;
  NiceMock<Event::MockDispatcher> dispatcher_;
  NiceMock<Runtime::MockLoader> runtime_;
  Event::MockTimer* interval_timer_ = new Event::MockTimer(&dispatcher_);
  Stats::IsolatedStoreImpl stats_store_;
  CallbackChecker checker_;
};

TEST_F(OutlierDetectorImplTest, BasicFlow) {
  EXPECT_CALL(cluster_, addMemberUpdateCb(_));
  cluster_.hosts_ = {HostPtr{new HostImpl(cluster_, "tcp://127.0.0.1:80", false, 1, "")}};
  EXPECT_CALL(*interval_timer_, enableTimer(std::chrono::milliseconds(10000)));
  TestOutlierDetectorImpl detector(cluster_, dispatcher_, runtime_, stats_store_);
  detector.addChangedStateCb([&](HostPtr host) -> void { checker_.check(host); });

  cluster_.hosts_.push_back(HostPtr{new HostImpl(cluster_, "tcp://127.0.0.1:81", false, 1, "")});
  cluster_.runCallbacks({cluster_.hosts_[1]}, {});

  // Cause a consecutive 5xx error.
  cluster_.hosts_[0]->outlierDetector().putHttpResponseCode(503);
  cluster_.hosts_[0]->outlierDetector().putHttpResponseCode(200);
  cluster_.hosts_[0]->outlierDetector().putResponseTime(std::chrono::milliseconds(5));
  cluster_.hosts_[0]->outlierDetector().putHttpResponseCode(503);
  cluster_.hosts_[0]->outlierDetector().putHttpResponseCode(503);
  cluster_.hosts_[0]->outlierDetector().putHttpResponseCode(503);
  cluster_.hosts_[0]->outlierDetector().putHttpResponseCode(503);

  EXPECT_CALL(detector, currentSystemTime())
      .WillOnce(Return(SystemTime(std::chrono::milliseconds(0))));
  EXPECT_CALL(checker_, check(cluster_.hosts_[0]));
  cluster_.hosts_[0]->outlierDetector().putHttpResponseCode(503);
  EXPECT_TRUE(cluster_.hosts_[0]->healthFlagGet(Host::HealthFlag::FAILED_OUTLIER_CHECK));

  EXPECT_EQ(1UL,
            stats_store_.gauge("cluster.fake_cluster.outlier_detection.ejections_active").value());

  // Interval that doesn't bring the host back in.
  EXPECT_CALL(detector, currentSystemTime())
      .WillOnce(Return(SystemTime(std::chrono::milliseconds(9999))));
  EXPECT_CALL(*interval_timer_, enableTimer(std::chrono::milliseconds(10000)));
  interval_timer_->callback_();

  // Interval that does bring the host back in.
  EXPECT_CALL(detector, currentSystemTime())
      .WillOnce(Return(SystemTime(std::chrono::milliseconds(30001))));
  EXPECT_CALL(checker_, check(cluster_.hosts_[0]));
  EXPECT_CALL(*interval_timer_, enableTimer(std::chrono::milliseconds(10000)));
  interval_timer_->callback_();
  EXPECT_FALSE(cluster_.hosts_[0]->healthFlagGet(Host::HealthFlag::FAILED_OUTLIER_CHECK));

  cluster_.runCallbacks({}, cluster_.hosts_);

  EXPECT_EQ(0UL,
            stats_store_.gauge("cluster.fake_cluster.outlier_detection.ejections_active").value());
  EXPECT_EQ(1UL,
            stats_store_.counter("cluster.fake_cluster.outlier_detection.ejections_total").value());
  EXPECT_EQ(1UL,
            stats_store_.counter("cluster.fake_cluster.outlier_detection.ejections_consecutive_5xx")
                .value());
}

} // Upstream
