// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Benchmark: replay a real _schemas topic dump through consume_to_store.
//
// Input file format (produced from a topic dump):
//   <record_count>\n
//   then per record:
//   <offset> <key_bytes> <value_bytes_or_-1>\n
//   <key json>\n
//   [<value json>\n]     (absent for tombstones, value_bytes == -1)
//
// Run with:
//   SCHEMAS_RECORDS=/path/to/schemas_dump.records ./replay_bench -- -c16
// SR_REPLAY_LIVE=1 replays through the live (eager) path instead of the
// staged path, for before/after comparison; the reported state fingerprint
// must be identical in both modes.

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/test/utils.h"
#include "storage/record_batch_builder.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>

namespace pps = pandaproxy::schema_registry;

namespace {

struct dump_record {
    int64_t offset;
    std::string key;
    std::optional<std::string> value;
};

std::vector<dump_record> load_dump(const std::string& path) {
    auto in = std::ifstream{path, std::ios::binary};
    BOOST_REQUIRE_MESSAGE(in.good(), "cannot open " + path);
    size_t count{};
    in >> count;
    in.ignore(); // newline
    auto recs = std::vector<dump_record>{};
    recs.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        int64_t offset{};
        int64_t key_len{};
        int64_t val_len{};
        in >> offset >> key_len >> val_len;
        in.ignore();
        auto rec = dump_record{.offset = offset};
        rec.key.resize(key_len);
        in.read(rec.key.data(), key_len);
        in.ignore();
        if (val_len >= 0) {
            auto val = std::string{};
            val.resize(val_len);
            in.read(val.data(), val_len);
            in.ignore();
            rec.value = std::move(val);
        }
        BOOST_REQUIRE_MESSAGE(in.good(), "truncated dump file");
        recs.push_back(std::move(rec));
    }
    return recs;
}

iobuf iobuf_from(const std::string& s) {
    auto buf = iobuf{};
    buf.append(s.data(), s.size());
    return buf;
}

} // namespace

SEASTAR_THREAD_TEST_CASE(replay_bench) {
    const char* path = std::getenv("SCHEMAS_RECORDS");
    if (path == nullptr) {
        BOOST_TEST_MESSAGE("SCHEMAS_RECORDS not set, skipping");
        return;
    }
    auto recs = load_dump(path);
    const bool live_mode = std::getenv("SR_REPLAY_LIVE") != nullptr;
    BOOST_TEST_MESSAGE(
      fmt::format(
        "loaded {} records, mode={}",
        recs.size(),
        live_mode ? "live" : "staged"));

    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    noop_transport dummy_transport;

    ss::sharded<pps::seq_writer> seq;
    seq
      .start(
        model::node_id{0},
        ss::default_smp_service_group(),
        std::ref(dummy_transport),
        std::reference_wrapper(s),
        ss::sharded_parameter(
          [] { return std::make_unique<sequence_state_checker_test>(); }))
      .get();
    auto stop_seq = ss::defer([&seq]() { seq.stop().get(); });

    auto c = pps::consume_to_store(
      s, seq.local(), live_mode ? pps::stage_defs::no : pps::stage_defs::yes);

    using clock = std::chrono::steady_clock;
    using dur = std::chrono::duration<double>;

    auto batches = std::vector<model::record_batch>{};
    batches.reserve(recs.size());
    for (const auto& r : recs) {
        auto rb = storage::record_batch_builder{
          model::record_batch_type::raft_data, model::offset{r.offset}};
        rb.add_raw_kv(
          iobuf_from(r.key),
          r.value.has_value() ? std::optional<iobuf>{iobuf_from(*r.value)}
                              : std::nullopt);
        batches.push_back(std::move(rb).build());
    }

    auto replay_start = clock::now();
    for (auto& b : batches) {
        c(std::move(b)).get();
    }
    auto replay_s = dur(clock::now() - replay_start).count();

    auto finalize_start = clock::now();
    if (!live_mode) {
        s.finalize_staged().get();
    }
    s.process_marked_schemas().get();
    auto finalize_s = dur(clock::now() - finalize_start).count();

    BOOST_TEST_MESSAGE(
      fmt::format(
        "replay: {:.3f}s finalize: {:.3f}s total: {:.3f}s",
        replay_s,
        finalize_s,
        replay_s + finalize_s));

    // State fingerprint: hash every (subject, version, id, deleted, def) so
    // staged and live modes can be compared for equality.
    auto subjects = s.get_subjects(pps::include_deleted::yes).get();
    std::sort(subjects.begin(), subjects.end());
    size_t fingerprint = 0;
    size_t n_versions = 0;
    auto mix = [&fingerprint](size_t h) {
        fingerprint ^= h + 0x9e3779b97f4a7c15ULL + (fingerprint << 6)
                       + (fingerprint >> 2);
    };
    auto dump = std::optional<std::ofstream>{};
    if (const char* dump_path = std::getenv("SR_BENCH_DUMP")) {
        dump.emplace(dump_path, std::ios::binary);
    }
    for (const auto& sub : subjects) {
        mix(std::hash<std::string_view>{}(std::string_view{sub.sub()}));
        auto versions = s.get_versions(sub, pps::include_deleted::yes).get();
        for (auto v : versions) {
            auto st
              = s.get_subject_schema(sub, v, pps::include_deleted::yes).get();
            ++n_versions;
            mix(static_cast<size_t>(st.version()));
            mix(static_cast<size_t>(st.id()));
            mix(static_cast<size_t>(bool(st.deleted)));
            auto linear = iobuf_to_bytes(st.schema.def().raw()());
            auto view = std::string_view{
              reinterpret_cast<const char*>(linear.data()), linear.size()};
            mix(std::hash<std::string_view>{}(view));
            if (dump) {
                *dump << "==== " << sub.sub() << " v" << st.version() << " id"
                      << st.id() << " del" << bool(st.deleted) << "\n"
                      << view << "\n";
            }
        }
    }
    BOOST_TEST_MESSAGE(
      fmt::format(
        "state: subjects={} versions={} fingerprint={:x}",
        subjects.size(),
        n_versions,
        fingerprint));
}
