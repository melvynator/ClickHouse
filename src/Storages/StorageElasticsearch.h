#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/URI.h>

#include <memory>
#include <string>
#include <vector>

namespace DB
{

struct ElasticsearchConfiguration
{
    Poco::URI uri;
    String index;
    String user;
    String password;
    String structure;
    String query;

    void checkHosts(const ContextPtr & context) const;
};

class ElasticsearchPITHolder
{
public:
    ElasticsearchPITHolder(const ElasticsearchConfiguration & config, const String & keep_alive);
    ~ElasticsearchPITHolder();

    const String & getId() const { return pit_id; }
    bool isOpenSearch() const { return is_opensearch; }

private:
    ElasticsearchConfiguration config;
    String pit_id;
    bool is_opensearch = false;
    LoggerPtr log;
};

using ElasticsearchPITHolderPtr = std::shared_ptr<ElasticsearchPITHolder>;

class StorageElasticsearch final : public IStorage
{
public:
    StorageElasticsearch(
        const StorageID & table_id_,
        const ElasticsearchConfiguration & configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "Elasticsearch"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    static ElasticsearchConfiguration getConfiguration(ASTs engine_args, ContextPtr context);

private:
    ElasticsearchConfiguration configuration;
    LoggerPtr log;
};

}
