/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/handlers/alter_client_quotas.h"

#include "cluster/config_frontend.h"
#include "kafka/protocol/schemata/alter_client_quotas_response.h"
#include "kafka/types.h"

#include <vector>

namespace kafka {

ss::sstring map_to_config_string(
  std::optional<std::unordered_map<ss::sstring, config::client_group_quota>>
    input) {
    if (!input.has_value()) {
        return "";
    }

    json::StringBuffer buffer;
    json::Writer<json::StringBuffer> writer(buffer);
    json::rjson_serialize(writer, *input);

    return ss::sstring{buffer.GetString()};
}

struct quotas_update {
    void upsert_default_produce_rate(int64_t value) {
        remove_default_produce = false;
        default_produce_rate = value;
    }

    void remove_default_produce_rate() {
        remove_default_produce = true;
        default_produce_rate = std::nullopt;
    }

    void upsert_default_fetch_rate(int64_t value) {
        remove_default_fetch = false;
        default_fetch_rate = value;
    }

    void remove_default_fetch_rate() {
        remove_default_fetch = true;
        default_fetch_rate = std::nullopt;
    }

    void upsert_default_controller_mutation_rate(int64_t value) {
        remove_default_controller_mutation = false;
        default_controller_mutation_rate = value;
    }

    void remove_default_controller_mutation_rate() {
        remove_default_controller_mutation = true;
        default_controller_mutation_rate = std::nullopt;
    }

    void upsert_client_produce_rate(ss::sstring client, int64_t value) {
        if (!client_produce_rate.has_value()) {
            auto current_v = config::shard_local_cfg()
                               .kafka_client_group_byte_rate_quota.bind();
            client_produce_rate = current_v();
        }

        auto it = std::find_if(
          client_produce_rate->begin(),
          client_produce_rate->end(),
          [client](auto& kv) { return kv.second.clients_prefix == client; });
        if (it != client_produce_rate->end()) {
            it->second.quota = value;
        } else {
            // TODO: how should we generate the key?
            auto keyt = ss::sstring{"kafka-" + client};
            (*client_produce_rate)[keyt] = config::client_group_quota{
              keyt, client, value};
        }
    }

    void remove_client_produce_rate(ss::sstring client) {
        if (!client_produce_rate.has_value()) {
            auto current_v = config::shard_local_cfg()
                               .kafka_client_group_byte_rate_quota.bind();
            client_produce_rate = current_v();
        }

        client_produce_rate->erase(client);
    }

    void upsert_client_fetch_rate(ss::sstring client, int64_t value) {
        if (!client_fetch_rate.has_value()) {
            auto current_v = config::shard_local_cfg()
                               .kafka_client_group_byte_rate_quota.bind();
            client_fetch_rate = current_v();
        }

        auto it = std::find_if(
          client_fetch_rate->begin(),
          client_fetch_rate->end(),
          [client](auto& kv) { return kv.second.clients_prefix == client; });
        if (it != client_fetch_rate->end()) {
            it->second.quota = value;
        } else {
            // TODO: how should we generate the key?
            auto keyt = ss::sstring{"kafka-" + client};
            (*client_fetch_rate)[keyt] = config::client_group_quota{
              keyt, client, value};
        }
    }

    void remove_client_fetch_rate(ss::sstring client) {
        if (!client_fetch_rate.has_value()) {
            auto current_v = config::shard_local_cfg()
                               .kafka_client_group_byte_rate_quota.bind();
            client_fetch_rate = current_v();
        }

        client_fetch_rate->erase(client);
    }

    template<typename T>
    ss::sstring opt_to_config_string(std::optional<T> value) {
        return value.has_value() ? ss::to_sstring(*value) : "";
    }

    void to_upserts(cluster::config_update_request& update) {
        // TODO: refer to config key with a constant reused here + in
        // configuration.cc
        if (remove_default_produce) {
            update.remove.emplace_back("target_quota_byte_rate");
        } else if (default_produce_rate.has_value()) {
            update.upsert.emplace_back(
              ss::sstring("target_quota_byte_rate"),
              ss::to_sstring(*default_produce_rate));
        }

        if (remove_default_fetch) {
            update.remove.emplace_back("target_fetch_quota_byte_rate");
        } else if (default_fetch_rate.has_value() || remove_default_fetch) {
            update.upsert.emplace_back(
              ss::sstring("target_fetch_quota_byte_rate"),
              ss::to_sstring(*default_fetch_rate));
        }

        if (remove_default_controller_mutation) {
            update.remove.emplace_back("kafka_admin_topic_api_rate");
        } else if (
          default_controller_mutation_rate.has_value()
          || remove_default_controller_mutation) {
            update.upsert.emplace_back(
              ss::sstring("kafka_admin_topic_api_rate"),
              ss::to_sstring(*default_controller_mutation_rate));
        }

        if (client_produce_rate.has_value()) {
            update.upsert.emplace_back(
              ss::sstring("kafka_client_group_byte_rate_quota"),
              map_to_config_string(client_produce_rate));
        }

        if (client_fetch_rate.has_value()) {
            update.upsert.emplace_back(
              ss::sstring("kafka_client_group_fetch_byte_rate_quota"),
              map_to_config_string(client_fetch_rate));
        }
    }

    // void to_response(cluster::config_update_request& update) {
    //     if (default_fetch_rate.has_value()) {
    //         update.upsert.emplace_back(
    //           ss::sstring("fetch_rate_blah"),
    //           ss::to_sstring(default_fetch_rate));
    //     }
    //     if (remove_default_fetch) {
    //         // TODO: is remove the right thing here?
    //         update.remove.emplace_back("fetch_rate_blah");
    //     }

    //     if (client_produce_rate.has_value()) {
    //         update.upsert.emplace_back(
    //           ss::sstring("client_produce_rate"),
    //           map_to_config_string(client_produce_rate));
    //     }
    // }

private:
    std::optional<int64_t> default_produce_rate;
    std::optional<int64_t> default_fetch_rate;
    std::optional<int64_t> default_controller_mutation_rate;
    std::optional<std::unordered_map<ss::sstring, config::client_group_quota>>
      client_fetch_rate;
    std::optional<std::unordered_map<ss::sstring, config::client_group_quota>>
      client_produce_rate;

    // TODO: maybe use tristate instead of value+bool
    bool remove_default_produce = false;
    bool remove_default_fetch = false;
    bool remove_default_controller_mutation = false;
};

std::vector<alter_client_quotas_response_entity_data> convert(
  const std::vector<alter_client_quotas_request_entity_data>& req_entity_data) {
    auto res_entity = std::vector<alter_client_quotas_response_entity_data>{};
    res_entity.reserve(req_entity_data.size());

    for (const auto& entity : req_entity_data) {
        res_entity.push_back(
          {.entity_type = entity.entity_type,
           .entity_name = entity.entity_name});
    }
    return res_entity;
}

void make_error_response(
  alter_client_quotas_request& req, alter_client_quotas_response& resp) {
    for (const auto& entry : req.data.entries) {
        // TODO: error handling
        //  * Do we just return a single error code for everything?
        //  * Do we return unsupported for things unsupported and internal
        //  server error for everything else?

        resp.data.entries.push_back(
          kafka::alter_client_quotas_response_entry_data{
            kafka::error_code::unknown_server_error,
            "Unknown server error",
            convert(entry.entity)});
    }
}

void make_success_response(
  alter_client_quotas_request& req, alter_client_quotas_response& resp) {
    for (const auto& entry : req.data.entries) {
        resp.data.entries.push_back(
          kafka::alter_client_quotas_response_entry_data{
            kafka::error_code::none, "", convert(entry.entity)});
    }
}

template<>
ss::future<response_ptr> alter_client_quotas_handler::handle(
  request_context ctx, ss::smp_service_group) {
    alter_client_quotas_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (!ctx.authorized(
          security::acl_operation::alter_configs,
          security::default_cluster_name)) {
        // TODO: fail
    }

    if (!ctx.audit()) {
        // TODO: fail
    }

    quotas_update update;
    bool any_unsupported = false;

    for (auto entry : request.data.entries) {
        vassert(entry.entity.size() == 1, "Should only contain client");
        if (entry.entity[0].entity_type != entity_type::client_id) {
            // TODO: fail
        }
        // TODO: for the default client, is the client "<default>" or is it
        // missing?
        auto client = entry.entity[0].entity_name;

        for (auto op : entry.ops) {
            if (!client.has_value()) {
                if (op.key == quota_type::consumer_byte_rate) {
                    if (!op.remove) {
                        update.upsert_default_fetch_rate(op.value);
                    } else {
                        update.remove_default_fetch_rate();
                    }
                } else if (op.key == quota_type::producer_byte_rate) {
                    if (!op.remove) {
                        update.upsert_default_produce_rate(op.value);
                    } else {
                        update.remove_default_produce_rate();
                    }
                } else if (op.key == quota_type::controller_mutation_rate) {
                    if (!op.remove) {
                        update.upsert_default_controller_mutation_rate(
                          op.value);
                    } else {
                        update.remove_default_controller_mutation_rate();
                    }
                } else {
                    any_unsupported = true;
                    break;
                }
            } else {
                if (op.key == quota_type::consumer_byte_rate) {
                    if (!op.remove) {
                        update.upsert_client_fetch_rate(*client, op.value);
                    } else {
                        update.remove_client_fetch_rate(*client);
                    }
                } else if (op.key == quota_type::producer_byte_rate) {
                    if (!op.remove) {
                        update.upsert_client_fetch_rate(*client, op.value);
                    } else {
                        update.remove_client_fetch_rate(*client);
                    }
                } else {
                    any_unsupported = true;
                    break;
                }
            }
        }

        if (any_unsupported) {
            break;
        }
    }

    alter_client_quotas_response response;

    if (any_unsupported) {
        // TODO: adapt error code
        make_error_response(request, response);
        co_return co_await ctx.respond(std::move(response));
    }

    if (!request.data.validate_only) {
        cluster::config_update_request cfg_update{};
        update.to_upserts(cfg_update);

        auto res = co_await ctx.config_frontend().local().patch(
          std::move(cfg_update), model::timeout_clock::now() + 5s);

        // TODO: feed back the to the client, probably set error on all the
        // inputs?

        if (res.errc) {
            make_error_response(request, response);
            co_return co_await ctx.respond(std::move(response));
        }
    }

    make_success_response(request, response);
    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
