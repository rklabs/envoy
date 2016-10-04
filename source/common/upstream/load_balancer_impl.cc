#include "load_balancer_impl.h"

#include "envoy/runtime/runtime.h"
#include "envoy/stats/stats.h"
#include "envoy/upstream/upstream.h"

#include "common/common/assert.h"

namespace Upstream {

const std::vector<HostPtr>& LoadBalancerBase::hostsToUse() {
  ASSERT(host_set_.healthyHosts().size() <= host_set_.hosts().size());
  if (host_set_.hosts().empty()) {
    return host_set_.hosts();
  }

  uint64_t global_panic_threshold =
      std::min(100UL, runtime_.snapshot().getInteger("upstream.healthy_panic_threshold", 50));
  double healthy_percent = 100.0 * host_set_.healthyHosts().size() / host_set_.hosts().size();

  // If the % of healthy hosts in the cluster is less than our panic threshold, we use all hosts.
  if (healthy_percent < global_panic_threshold) {
    stats_.upstream_rq_lb_healthy_panic_.inc();
    return host_set_.hosts();
  }

  // Early exit if we cannot perform zone aware routing.
  if (stats_.upstream_zone_count_.value() < 2 || host_set_.localZoneHealthyHosts().empty() ||
      !runtime_.snapshot().featureEnabled("upstream.zone_routing.enabled", 100)) {
    return host_set_.healthyHosts();
  }

  // Do not perform zone routing for small clusters
  if (host_set_.healthyHosts().size() <
      runtime_.snapshot().getInteger("upstream.zone_routing.min_cluster_size", 6)) {
    stats_.zone_cluster_too_small_.inc();
    return host_set_.healthyHosts();
  }

  double zone_host_percent =
      100.0 * host_set_.localZoneHealthyHosts().size() / host_set_.healthyHosts().size();
  double zone_expected_percent = 100.0 / stats_.upstream_zone_count_.value();

  // If local zone percent is higher than expected, we can route whole current zone traffic and
  // will also get some cross zone traffic from other zones.
  if (zone_host_percent >= zone_expected_percent) {
    stats_.zone_over_percentage_.inc();
    return host_set_.localZoneHealthyHosts();
  }

  // If current zone percent is lower than expected we should partially route requests from the same
  // zone. Scale by 100 for better precision.
  const uint64_t scale_factor = 100;
  uint64_t req_percent_to_route =
      static_cast<uint64_t>(scale_factor * 100 * zone_host_percent / zone_expected_percent);
  if (random_.random() % 10000 < req_percent_to_route) {
    stats_.zone_routing_sampled_.inc();
    return host_set_.localZoneHealthyHosts();
  } else {
    stats_.zone_routing_no_sampled_.inc();
    return host_set_.healthyHosts();
  }
}

ConstHostPtr RoundRobinLoadBalancer::chooseHost() {
  const std::vector<HostPtr>& hosts_to_use = hostsToUse();
  if (hosts_to_use.empty()) {
    return nullptr;
  }

  return hosts_to_use[rr_index_++ % hosts_to_use.size()];
}

LeastRequestLoadBalancer::LeastRequestLoadBalancer(const HostSet& host_set, ClusterStats& stats,
                                                   Runtime::Loader& runtime,
                                                   Runtime::RandomGenerator& random)
    : LoadBalancerBase(host_set, stats, runtime, random) {
  host_set.addMemberUpdateCb(
      [this](const std::vector<HostPtr>&, const std::vector<HostPtr>& hosts_removed) -> void {
        if (last_host_) {
          for (const HostPtr& host : hosts_removed) {
            if (host == last_host_) {
              hits_left_ = 0;
              last_host_.reset();

              break;
            }
          }
        }
      });
}

ConstHostPtr LeastRequestLoadBalancer::chooseHost() {
  bool is_weight_imbalanced = stats_.max_host_weight_.value() != 1;
  bool is_weight_enabled = runtime_.snapshot().getInteger("upstream.weight_enabled", 1UL) != 0;

  if (is_weight_imbalanced && hits_left_ > 0 && is_weight_enabled) {
    --hits_left_;

    return last_host_;
  } else {
    // To avoid hit stale last_host_ when all hosts become weight balanced.
    hits_left_ = 0;
    last_host_.reset();
  }

  const std::vector<HostPtr>& hosts_to_use = hostsToUse();
  if (hosts_to_use.empty()) {
    return nullptr;
  }

  // Make weighed random if we have hosts with non 1 weights.
  if (is_weight_imbalanced & is_weight_enabled) {
    last_host_ = hosts_to_use[random_.random() % hosts_to_use.size()];
    hits_left_ = last_host_->weight() - 1;

    return last_host_;
  } else {
    HostPtr host1 = hosts_to_use[random_.random() % hosts_to_use.size()];
    HostPtr host2 = hosts_to_use[random_.random() % hosts_to_use.size()];
    if (host1->stats().rq_active_.value() < host2->stats().rq_active_.value()) {
      return host1;
    } else {
      return host2;
    }
  }
}

ConstHostPtr RandomLoadBalancer::chooseHost() {
  const std::vector<HostPtr>& hosts_to_use = hostsToUse();
  if (hosts_to_use.empty()) {
    return nullptr;
  }

  return hosts_to_use[random_.random() % hosts_to_use.size()];
}

} // Upstream
