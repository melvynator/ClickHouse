#include <Storages/StorageElasticsearch.h>
#include <Processors/Sources/ElasticsearchSource.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <QueryPipeline/Pipe.h>
#include <Common/parseAddress.h>
#include <Common/logger_useful.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
}

namespace
{

String httpRequest(const Poco::URI & uri, const String & user, const String & password,
                   const String & path, const String & method, const String & body = "")
{
    auto target = uri;
    target.setPath(path);
    Poco::Net::HTTPBasicCredentials credentials(user, password);

    auto builder = BuilderRWBufferFromHTTP(target)
        .withConnectionGroup(HTTPConnectionGroupType::HTTP)
        .withMethod(method);

    if (!body.empty())
    {
        auto body_callback = [&body](std::ostream & os) { os << body; };
        builder.withOutCallback(std::move(body_callback));
    }

    auto buf = builder.create(credentials);

    String result;
    readStringUntilEOF(result, *buf);
    return result;
}

}

void ElasticsearchConfiguration::checkHosts(const ContextPtr & context) const
{
    context->getRemoteHostFilter().checkURL(uri);
}

ElasticsearchPITHolder::ElasticsearchPITHolder(const ElasticsearchConfiguration & config_, const String & keep_alive)
    : config(config_)
    , log(getLogger("ElasticsearchPIT"))
{
    Poco::Net::HTTPBasicCredentials credentials(config.user, config.password);
    HTTPHeaderEntries headers = {{"Content-Type", "application/json"}};

    // Try ES 8.x _pit endpoint first
    String response;
    bool tried_es = false;
    try
    {
        auto target = config.uri;
        target.setPath("/" + config.index + "/_pit");
        target.setQuery("keep_alive=" + keep_alive);
        auto buf = BuilderRWBufferFromHTTP(target)
            .withConnectionGroup(HTTPConnectionGroupType::HTTP)
            .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
            .withHeaders(headers)
            .create(credentials);
        readStringUntilEOF(response, *buf);
        tried_es = true;
    }
    catch (...)
    {
        // Fall back to OpenSearch _search/point_in_time endpoint
        auto target = config.uri;
        target.setPath("/" + config.index + "/_search/point_in_time");
        target.setQuery("keep_alive=" + keep_alive);
        auto buf = BuilderRWBufferFromHTTP(target)
            .withConnectionGroup(HTTPConnectionGroupType::HTTP)
            .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
            .withHeaders(headers)
            .create(credentials);
        readStringUntilEOF(response, *buf);
        is_opensearch = true;
        LOG_DEBUG(log, "Detected OpenSearch (using _search/point_in_time)");
    }

    auto id_pos = response.find("\"pit_id\"");
    if (id_pos != String::npos)
    {
        is_opensearch = true;
    }
    else
    {
        id_pos = response.find("\"id\"");
    }
    if (id_pos == String::npos)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Failed to open PIT: {}", response);

    auto colon_pos = response.find(static_cast<char>(':'), id_pos);
    auto quote_start = response.find(static_cast<char>(0x22), colon_pos + 1);
    auto quote_end = response.find(static_cast<char>(0x22), quote_start + 1);
    if (quote_start == String::npos || quote_end == String::npos)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Failed to parse PIT id from response: {}", response);

    pit_id = response.substr(quote_start + 1, quote_end - quote_start - 1);
    LOG_DEBUG(log, "Opened PIT (length={})", pit_id.size());
}

ElasticsearchPITHolder::~ElasticsearchPITHolder()
{
    try
    {
        String body = is_opensearch
            ? "{\"pit_id\":\"" + pit_id + "\"}"
            : "{\"id\":\"" + pit_id + "\"}";
        auto target = config.uri;
        target.setPath(is_opensearch ? "/_search/point_in_time" : "/_pit");
        Poco::Net::HTTPBasicCredentials credentials(config.user, config.password);
        auto body_callback = [&body](std::ostream & os) { os << body; };
        HTTPHeaderEntries del_headers = {{"Content-Type", "application/json"}};
        auto buf = BuilderRWBufferFromHTTP(target)
            .withConnectionGroup(HTTPConnectionGroupType::HTTP)
            .withMethod(Poco::Net::HTTPRequest::HTTP_DELETE)
            .withOutCallback(std::move(body_callback))
            .withHeaders(std::move(del_headers))
            .create(credentials);
        String resp;
        readStringUntilEOF(resp, *buf);
        LOG_DEBUG(log, "Closed PIT successfully");
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to close PIT");
    }
}

StorageElasticsearch::StorageElasticsearch(
    const StorageID & table_id_,
    const ElasticsearchConfiguration & configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage(table_id_)
    , configuration(configuration_)
    , log(getLogger("StorageElasticsearch (" + table_id_.getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageElasticsearch::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    size_t page_size = 10000;
    String keep_alive = "5m";
    size_t max_retries = 3;
    size_t connection_timeout_ms = 10000;
    size_t request_timeout_ms = 30000;

    auto pit_holder = std::make_shared<ElasticsearchPITHolder>(configuration, keep_alive);

    if (num_streams == 0)
        num_streams = 1;

    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<ElasticsearchSource>(
            configuration,
            pit_holder,
            i,
            num_streams,
            sample_block,
            max_block_size,
            page_size,
            keep_alive,
            max_retries,
            connection_timeout_ms,
            request_timeout_ms,
            pit_holder->isOpenSearch()));
    }

    return Pipe::unitePipes(std::move(pipes));
}

ElasticsearchConfiguration StorageElasticsearch::getConfiguration(ASTs engine_args, ContextPtr context)
{
    ElasticsearchConfiguration config;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        config.uri = Poco::URI(named_collection->get<String>("host"));
        config.index = named_collection->get<String>("index");
        config.user = named_collection->get<String>("user");
        config.password = named_collection->get<String>("password");
        config.structure = named_collection->get<String>("structure");
        config.query = named_collection->getOrDefault<String>("query", "");
        return config;
    }

    for (auto & arg : engine_args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    if (engine_args.size() < 5 || engine_args.size() > 6)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function elasticsearch requires 5 or 6 arguments: "
            "elasticsearch(host, index, user, password, structure [, query])");

    config.uri = Poco::URI(checkAndGetLiteralArgument<String>(engine_args[0], "host"));
    config.index = checkAndGetLiteralArgument<String>(engine_args[1], "index");
    config.user = checkAndGetLiteralArgument<String>(engine_args[2], "user");
    config.password = checkAndGetLiteralArgument<String>(engine_args[3], "password");
    config.structure = checkAndGetLiteralArgument<String>(engine_args[4], "structure");

    if (engine_args.size() == 6)
        config.query = checkAndGetLiteralArgument<String>(engine_args[5], "query");

    auto scheme = config.uri.getScheme();
    if (scheme != "http" && scheme != "https")
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Elasticsearch host must start with http:// or https://, got: {}", config.uri.toString());

    if (config.index.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch index cannot be empty");

    if (config.structure.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch structure cannot be empty");

    config.checkHosts(context);
    return config;
}


void registerStorageElasticsearch(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .source_access_type = AccessTypeObjects::Source::ELASTICSEARCH,
    };

    factory.registerStorage("Elasticsearch", [](const StorageFactory::Arguments & args)
    {
        auto config = StorageElasticsearch::getConfiguration(args.engine_args, args.getLocalContext());
        return std::make_shared<StorageElasticsearch>(
            args.table_id,
            config,
            args.columns,
            args.constraints,
            args.comment);
    }, features);
}

}
