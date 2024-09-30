// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_cache.h"
#include "cluster/shard_table.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "config/configuration.h"
#include "features/feature_table_snapshot.h"
#include "model/metadata.h"
#include "test_utils/fixture.h"
#include "utils/unresolved_address.h"

using namespace std::chrono_literals; // NOLINT

FIXTURE_TEST(test_join_single_node, cluster_test_fixture) {
    model::node_id id{0};
    create_node_application(id);
    wait_for_controller_leadership(id).get();

    wait_for_all_members(3s).get();

    auto brokers = get_local_cache(model::node_id{0}).nodes();

    // single broker
    BOOST_REQUIRE_EQUAL(brokers.size(), 1);
    BOOST_REQUIRE(brokers.contains(model::node_id(0)));
}

FIXTURE_TEST(test_two_node_cluster, cluster_test_fixture) {
    create_node_application(model::node_id{0});
    create_node_application(model::node_id{1});
    // Check if all brokers were registered
    wait_for_all_members(3s).get();
}

FIXTURE_TEST(test_three_node_cluster, cluster_test_fixture) {
    create_node_application(model::node_id{0});
    create_node_application(model::node_id{1});
    create_node_application(model::node_id{2});

    wait_for_all_members(3s).get();
}

FIXTURE_TEST(test_auto_assign_node_id, cluster_test_fixture) {
    create_node_application(model::node_id{0}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(0, *config::node().node_id());

    create_node_application(model::node_id{1}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(1, *config::node().node_id());

    create_node_application(model::node_id{2}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(2, *config::node().node_id());

    wait_for_all_members(3s).get();
}

FIXTURE_TEST(test_auto_assign_non_seeds, cluster_test_fixture) {
    create_node_application(model::node_id{0});
    BOOST_REQUIRE_EQUAL(0, *config::node().node_id());

    create_node_application(model::node_id{1}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(1, *config::node().node_id());

    create_node_application(model::node_id{2}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(2, *config::node().node_id());

    wait_for_all_members(3s).get();
}

FIXTURE_TEST(test_auto_assign_with_explicit_node_id, cluster_test_fixture) {
    create_node_application(model::node_id{0});
    BOOST_REQUIRE_EQUAL(0, *config::node().node_id());

    // Explicitly assign node ID 2. Node ID assignment should assign around it.
    create_node_application(model::node_id{2});
    BOOST_REQUIRE_EQUAL(2, *config::node().node_id());

    create_node_application(model::node_id{1}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(1, *config::node().node_id());

    create_node_application(model::node_id{3}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(3, *config::node().node_id());

    wait_for_all_members(3s).get();
}

FIXTURE_TEST(
  test_seed_driven_cluster_bootstrap_single_node, cluster_test_fixture) {
    const model::node_id id0{0};
    create_node_application(
      id0, configure_node_id::no, empty_seed_starts_cluster::no);
    BOOST_REQUIRE_EQUAL(0, *config::node().node_id());
    wait_for_controller_leadership(id0).get();
    wait_for_all_members(3s).get();

    auto brokers = get_local_cache(model::node_id{0}).nodes();

    // single broker
    BOOST_REQUIRE_EQUAL(brokers.size(), 1);
    BOOST_REQUIRE(brokers.contains(id0));
}

FIXTURE_TEST(test_feature_table_snapshots, cluster_test_fixture) {
    // Switch on the dummy test features for use in the test.  It is safe
    // to leave this set for the remainder of process lifetime, as other
    // tests should not notice or care that there is an extra feature flag.
    setenv("__REDPANDA_TEST_FEATURES", "true", 1);

    // Start up a node normally
    const model::node_id id0{0};
    auto app = create_node_application(
      id0, configure_node_id::no, empty_seed_starts_cluster::no);
    BOOST_REQUIRE_EQUAL(0, *config::node().node_id());
    wait_for_controller_leadership(id0).get();
    wait_for_all_members(3s).get();

    // Wait til it creates a snapshot: this requires a wait because the feature
    // manager progress is asynchronous when translating a node version update
    // into an update to the feature table to update the cluster version.
    tests::cooperative_spin_wait_with_timeout(5000ms, [app] {
        auto snap_opt = app->storage.local().kvs().get(
          storage::kvstore::key_space::controller,
          features::feature_table_snapshot::kvstore_key());
        return snap_opt.has_value();
    }).get();

    auto active_version = app->feature_table.local().get_active_version();

    // Restart it
    remove_node_application(id0);
    app = create_node_application(
      id0, configure_node_id::no, empty_seed_starts_cluster::no);

    // Peek at its feature table to check that it is in the expected state,
    // although this doesn't prove it was loaded from a snapshot.
    BOOST_REQUIRE(
      active_version == app->feature_table.local().get_active_version());
    BOOST_REQUIRE(
      app->feature_table.local().is_active(features::feature::test_alpha)
      == false);

    // Now inject a phony snapshot to check that the application really is
    // loading from the snapshot and not just reconstituting via controller log.
    features::feature_table_snapshot bogus_snapshot;
    bogus_snapshot.applied_offset
      = app->feature_table.local().get_applied_offset();
    bogus_snapshot.version = app->feature_table.local().get_active_version();
    bogus_snapshot.states = {features::feature_state_snapshot{
      .name = "__test_alpha", .state = features::feature_state::state::active}};
    app->storage.local()
      .kvs()
      .put(
        storage::kvstore::key_space::controller,
        features::feature_table_snapshot::kvstore_key(),
        serde::to_iobuf(bogus_snapshot))
      .get();

    remove_node_application(id0);
    app = create_node_application(
      id0, configure_node_id::no, empty_seed_starts_cluster::no);

    BOOST_REQUIRE(
      app->feature_table.local().is_active(features::feature::test_alpha)
      == true);
}

FIXTURE_TEST(
  test_shadow_indexing_update_topic_properties, cluster_test_fixture) {
    // add two nodes
    auto controller_id = model::node_id{0};
    application* node_0 = create_node_application(controller_id);
    wait_for_controller_leadership(controller_id).get();
    application* node_1 = create_node_application(model::node_id{1});

    const model::topic topic{"tapioca"};
    const model::topic_namespace tp_ns{model::kafka_namespace, topic};
    auto* rp = instance(controller_id);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::disabled;
    rp->add_topic(tp_ns, 1, props, 2).get();

    // Simulate a case in which a lower version node makes an alter config call,
    // where shadow_indexing_mode has an update and remote_read/remote_write do
    // not. This will be redirected to the controller node, and the deprecated
    // path in topic_table::apply() should be taken.
    {
        auto update = cluster::topic_properties_update{
          {model::kafka_namespace, topic}};
        update.properties.get_shadow_indexing().op
          = cluster::incremental_update_operation::set;
        update.properties.get_shadow_indexing().value
          = model::shadow_indexing_mode::full;
        auto res = node_1->controller->get_topics_frontend()
                     .local()
                     .update_topic_properties(
                       {update}, model::timeout_clock().now() + 5s)
                     .get();

        BOOST_REQUIRE_EQUAL(res.size(), 1);
        BOOST_REQUIRE_EQUAL(res[0].ec, cluster::errc::success);
        auto cfg = node_0->controller->get_topics_state().local().get_topic_cfg(
          tp_ns);
        BOOST_REQUIRE(cfg.has_value());
        const auto& cfg_shadow_indexing
          = cfg.value().properties.shadow_indexing;
        BOOST_REQUIRE(cfg_shadow_indexing.has_value());
        BOOST_REQUIRE_EQUAL(
          cfg_shadow_indexing.value(), model::shadow_indexing_mode::full);
    }

    // Simulate a case in which an updated node makes an alter config
    // call, where shadow_indexing_mode has an update, as well as
    // remote_read/remote_write. This will be redirected to the controller node,
    // and the non-deprecated path in topic_table::apply() should be taken.
    {
        auto update = cluster::topic_properties_update{
          {model::kafka_namespace, topic}};
        update.properties.get_shadow_indexing().op
          = cluster::incremental_update_operation::set;
        update.properties.get_shadow_indexing().value
          = model::shadow_indexing_mode::disabled;

        update.properties.remote_read.op
          = cluster::incremental_update_operation::set;
        update.properties.remote_read.value = false;

        update.properties.remote_write.op
          = cluster::incremental_update_operation::set;
        update.properties.remote_write.value = false;

        auto res = node_1->controller->get_topics_frontend()
                     .local()
                     .update_topic_properties(
                       {update}, model::timeout_clock().now() + 5s)
                     .get();

        BOOST_REQUIRE_EQUAL(res.size(), 1);
        BOOST_REQUIRE_EQUAL(res[0].ec, cluster::errc::success);
        auto cfg = node_0->controller->get_topics_state().local().get_topic_cfg(
          tp_ns);
        BOOST_REQUIRE(cfg.has_value());
        const auto& cfg_shadow_indexing
          = cfg.value().properties.shadow_indexing;
        BOOST_REQUIRE(cfg_shadow_indexing.has_value());
        BOOST_REQUIRE_EQUAL(
          cfg_shadow_indexing.value(), model::shadow_indexing_mode::disabled);
    }

    // Simulate a case in which an updated node makes an alter config
    // call, where shadow_indexing_mode has an update, as well as
    // remote_read/remote_write. This will be redirected to the controller node,
    // and the non-deprecated path in topic_table::apply() should be taken.
    // We should be able to set true/false simultaneously this way.
    {
        // This update is set to "disabled", but is ignored by the topic_table
        // as it follows remote_read/write updates instead.
        auto update = cluster::topic_properties_update{
          {model::kafka_namespace, topic}};
        update.properties.get_shadow_indexing().op
          = cluster::incremental_update_operation::set;
        update.properties.get_shadow_indexing().value
          = model::shadow_indexing_mode::disabled;

        update.properties.remote_read.op
          = cluster::incremental_update_operation::set;
        update.properties.remote_read.value = true;

        update.properties.remote_write.op
          = cluster::incremental_update_operation::set;
        update.properties.remote_write.value = false;

        auto res = node_1->controller->get_topics_frontend()
                     .local()
                     .update_topic_properties(
                       {update}, model::timeout_clock().now() + 5s)
                     .get();

        BOOST_REQUIRE_EQUAL(res.size(), 1);
        BOOST_REQUIRE_EQUAL(res[0].ec, cluster::errc::success);
        auto cfg = node_0->controller->get_topics_state().local().get_topic_cfg(
          tp_ns);
        BOOST_REQUIRE(cfg.has_value());
        const auto& cfg_shadow_indexing
          = cfg.value().properties.shadow_indexing;
        BOOST_REQUIRE(cfg_shadow_indexing.has_value());
        BOOST_REQUIRE_EQUAL(
          cfg_shadow_indexing.value(), model::shadow_indexing_mode::fetch);
    }
}
