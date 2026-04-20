#pragma once

#include <Processors/ISource.h>
#include <Storages/StorageElasticsearch.h>
#include <IO/ConnectionTimeouts.h>

#include <string>
#include <vector>

namespace DB
{

class ElasticsearchSource final : public ISource
{
public:
    ElasticsearchSource(
        const ElasticsearchConfiguration & config_,
        ElasticsearchPITHolderPtr pit_holder_,
        size_t slice_id_,
        size_t slice_max_,
        const Block & sample_block_,
        size_t max_block_size_,
        size_t page_size_,
        const String & keep_alive_,
        size_t max_retries_,
        size_t connection_timeout_ms_,
        size_t request_timeout_ms_,
        bool is_opensearch_);

    String getName() const override { return "Elasticsearch"; }

private:
    Chunk generate() override;

    String buildRequestBody() const;
    void parseResponse(const String & response_body, MutableColumns & columns, const Block & header);
    String executeSearchRequest(const String & request_body);

    ElasticsearchConfiguration config;
    ElasticsearchPITHolderPtr pit_holder;

    size_t slice_id;
    size_t slice_max;
    size_t max_block_size;
    size_t page_size;
    String keep_alive;
    size_t max_retries;
    size_t connection_timeout_ms;
    size_t request_timeout_ms;

    std::vector<String> source_fields;
    std::vector<String> last_sort_values;
    bool is_first_request = true;
    bool all_read = false;

    bool need_id = false;
    bool need_index = false;
    bool need_score = false;
    bool is_opensearch = false;

    LoggerPtr log;
};

}
