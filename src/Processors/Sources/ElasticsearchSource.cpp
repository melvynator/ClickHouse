#include <Processors/Sources/ElasticsearchSource.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>
#include <base/sleep.h>

#include <simdjson.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
    extern const int AUTHENTICATION_FAILED;
}

ElasticsearchSource::ElasticsearchSource(
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
    bool is_opensearch_)
    : ISource(std::make_shared<Block>(sample_block_))
    , config(config_)
    , pit_holder(std::move(pit_holder_))
    , slice_id(slice_id_)
    , slice_max(slice_max_)
    , max_block_size(max_block_size_)
    , page_size(page_size_)
    , keep_alive(keep_alive_)
    , max_retries(max_retries_)
    , connection_timeout_ms(connection_timeout_ms_)
    , request_timeout_ms(request_timeout_ms_)
    , is_opensearch(is_opensearch_)
    , log(getLogger("ElasticsearchSource"))
{
    for (size_t i = 0; i < sample_block_.columns(); ++i)
    {
        const auto & col_name = sample_block_.getByPosition(i).name;
        if (col_name == "_id")
            need_id = true;
        else if (col_name == "_index")
            need_index = true;
        else if (col_name == "_score")
            need_score = true;
        else
            source_fields.push_back(col_name);
    }
}

String ElasticsearchSource::buildRequestBody() const
{
    WriteBufferFromOwnString buf;

    writeCString("{\"size\":", buf);
    writeIntText(page_size, buf);
    writeCString(",\"pit\":{\"id\":\"", buf);
    writeString(pit_holder->getId(), buf);
    writeCString("\",\"keep_alive\":\"", buf);
    writeString(keep_alive, buf);
    writeCString("\"}", buf);
    writeCString(is_opensearch ? ",\"sort\":[\"_doc\"]" : ",\"sort\":[\"_shard_doc\"]", buf);

    if (slice_max > 1)
    {
        writeCString(",\"slice\":{\"id\":", buf);
        writeIntText(slice_id, buf);
        writeCString(",\"max\":", buf);
        writeIntText(slice_max, buf);
        writeChar('}', buf);
    }

    if (!is_first_request && !last_sort_values.empty())
    {
        writeCString(",\"search_after\":[", buf);
        for (size_t i = 0; i < last_sort_values.size(); ++i)
        {
            if (i > 0) writeChar(',', buf);
            writeString(last_sort_values[i], buf);
        }
        writeChar(']', buf);
    }

    if (!source_fields.empty())
    {
        writeCString(",\"_source\":[", buf);
        for (size_t i = 0; i < source_fields.size(); ++i)
        {
            if (i > 0) writeChar(',', buf);
            writeChar('"', buf);
            writeString(source_fields[i], buf);
            writeChar('"', buf);
        }
        writeChar(']', buf);
    }

    if (!config.query.empty())
    {
        writeCString(",\"query\":", buf);
        writeString(config.query, buf);
    }
    else
        writeCString(",\"query\":{\"match_all\":{}}", buf);

    writeChar('}', buf);

    return buf.str();
}

String ElasticsearchSource::executeSearchRequest(const String & request_body)
{
    auto target = config.uri;
    target.setPath("/_search");
    Poco::Net::HTTPBasicCredentials credentials(config.user, config.password);

    for (size_t attempt = 0; attempt <= max_retries; ++attempt)
    {
        try
        {
            auto body_callback = [&request_body](std::ostream & os) { os << request_body; };

            HTTPHeaderEntries headers = {{"Content-Type", "application/json"}};
            auto http_buf = BuilderRWBufferFromHTTP(target)
                .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                .withOutCallback(std::move(body_callback))
                .withHeaders(std::move(headers))
                .create(credentials);

            String result;
            readStringUntilEOF(result, *http_buf);
            return result;
        }
        catch (const Exception & e)
        {
            if (attempt < max_retries)
            {
                auto delay_ms = 100u * (1u << attempt);
                LOG_WARNING(log, "ES request failed (attempt {}/{}): {}. Retrying in {}ms.",
                    attempt + 1, max_retries + 1, e.message(), delay_ms);
                sleepForMilliseconds(delay_ms);
                continue;
            }
            throw;
        }
    }
    __builtin_unreachable();
}

namespace
{

void insertValueToColumn(IColumn & column, const DataTypePtr & type, simdjson::ondemand::value value)
{
    auto type_id = type->getTypeId();
    bool is_nullable = type->isNullable();
    const auto * nullable_type = is_nullable ? typeid_cast<const DataTypeNullable *>(type.get()) : nullptr;
    auto nested_type_id = nullable_type ? nullable_type->getNestedType()->getTypeId() : type_id;

    if (value.type() == simdjson::ondemand::json_type::null)
    {
        column.insertDefault();
        return;
    }

    if (is_nullable)
    {
        auto & nullable_col = assert_cast<ColumnNullable &>(column);
        auto & nested_col = nullable_col.getNestedColumn();
        auto & null_map = nullable_col.getNullMapData();
        auto nested_type = nullable_type->getNestedType();
        insertValueToColumn(nested_col, nested_type, value);
        null_map.push_back(0);
        return;
    }

    switch (nested_type_id)
    {
        case TypeIndex::UInt8:
            if (value.type() == simdjson::ondemand::json_type::boolean)
                assert_cast<ColumnUInt8 &>(column).insertValue(bool(value) ? 1 : 0);
            else
                assert_cast<ColumnUInt8 &>(column).insertValue(static_cast<UInt8>(uint64_t(value)));
            break;
        case TypeIndex::UInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(static_cast<UInt16>(uint64_t(value)));
            break;
        case TypeIndex::UInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(uint64_t(value)));
            break;
        case TypeIndex::UInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(uint64_t(value));
            break;
        case TypeIndex::Int8:
            assert_cast<ColumnInt8 &>(column).insertValue(static_cast<Int8>(int64_t(value)));
            break;
        case TypeIndex::Int16:
            assert_cast<ColumnInt16 &>(column).insertValue(static_cast<Int16>(int64_t(value)));
            break;
        case TypeIndex::Int32:
            assert_cast<ColumnInt32 &>(column).insertValue(static_cast<Int32>(int64_t(value)));
            break;
        case TypeIndex::Int64:
            assert_cast<ColumnInt64 &>(column).insertValue(int64_t(value));
            break;
        case TypeIndex::Float32:
            assert_cast<ColumnFloat32 &>(column).insertValue(static_cast<Float32>(double(value)));
            break;
        case TypeIndex::Float64:
            assert_cast<ColumnFloat64 &>(column).insertValue(double(value));
            break;
        case TypeIndex::String:
        {
            if (value.type() == simdjson::ondemand::json_type::string)
            {
                std::string_view sv = value.get_string().value();
                assert_cast<ColumnString &>(column).insertData(sv.data(), sv.size());
            }
            else
            {
                std::string_view sv = simdjson::to_json_string(value).value();
                assert_cast<ColumnString &>(column).insertData(sv.data(), sv.size());
            }
            break;
        }
        case TypeIndex::DateTime:
        {
            if (value.type() == simdjson::ondemand::json_type::number)
            {
                auto epoch_sec = uint64_t(value);
                assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(epoch_sec));
            }
            else if (value.type() == simdjson::ondemand::json_type::string)
            {
                std::string_view sv = value.get_string().value();
                String date_str(sv.data(), sv.size());
                ReadBufferFromString in(date_str);
                time_t t = 0;
                readDateTimeText(t, in, DateLUT::instance("UTC"));
                assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(t));
            }
            else
                column.insertDefault();
            break;
        }
        default:
        {
            if (value.type() == simdjson::ondemand::json_type::string)
            {
                std::string_view sv = value.get_string().value();
                assert_cast<ColumnString &>(column).insertData(sv.data(), sv.size());
            }
            else
            {
                std::string_view sv = simdjson::to_json_string(value).value();
                assert_cast<ColumnString &>(column).insertData(sv.data(), sv.size());
            }
            break;
        }
    }
}

}

void ElasticsearchSource::parseResponse(const String & response_body, MutableColumns & columns, const Block & header)
{
    simdjson::ondemand::parser parser;
    simdjson::padded_string padded(response_body);
    auto doc = parser.iterate(padded);

    auto error_field = doc.find_field("error");
    if (error_field.error() == simdjson::SUCCESS)
    {
        std::string_view ej = simdjson::to_json_string(error_field.value()).value();
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch error: {}", String(ej.data(), ej.size()));
    }

    auto hits_obj = doc["hits"];
    auto hits_array = hits_obj["hits"];

    last_sort_values.clear();
    size_t row_count = 0;

    for (auto hit : hits_array)
    {
        auto hit_obj = hit.get_object().value();

        for (size_t col_idx = 0; col_idx < header.columns(); ++col_idx)
        {
            const auto & col_name = header.getByPosition(col_idx).name;
            const auto & col_type = header.getByPosition(col_idx).type;

            if (col_name == "_id")
            {
                std::string_view id_val = hit_obj["_id"];
                assert_cast<ColumnString &>(*columns[col_idx]).insertData(id_val.data(), id_val.size());
            }
            else if (col_name == "_index")
            {
                std::string_view idx_val = hit_obj["_index"];
                assert_cast<ColumnString &>(*columns[col_idx]).insertData(idx_val.data(), idx_val.size());
            }
            else if (col_name == "_score")
            {
                auto score_result = hit_obj["_score"];
                if (score_result.error() == simdjson::SUCCESS
                    && score_result.type() == simdjson::ondemand::json_type::number)
                    assert_cast<ColumnFloat32 &>(*columns[col_idx]).insertValue(
                        static_cast<Float32>(double(score_result.value())));
                else
                    columns[col_idx]->insertDefault();
            }
            else
            {
                auto source_obj = hit_obj["_source"].get_object().value();
                auto field_result = source_obj[col_name];

                if (field_result.error() != simdjson::SUCCESS)
                    columns[col_idx]->insertDefault();
                else
                {
                    try { insertValueToColumn(*columns[col_idx], col_type, field_result.value()); }
                    catch (...) { columns[col_idx]->insertDefault(); }
                }
            }
        }

        auto sort_array = hit_obj["sort"];
        last_sort_values.clear();
        for (auto sort_val : sort_array)
        {
            std::string_view sv = simdjson::to_json_string(sort_val.value()).value();
            last_sort_values.emplace_back(sv.data(), sv.size());
        }

        ++row_count;
    }

    if (row_count == 0)
        all_read = true;

    LOG_DEBUG(log, "Parsed {} rows from Elasticsearch (slice {}/{})", row_count, slice_id, slice_max);
}

Chunk ElasticsearchSource::generate()
{
    if (all_read)
        return {};

    String request_body = buildRequestBody();
    is_first_request = false;

    LOG_TRACE(log, "ES request (slice {}/{}): {}", slice_id, slice_max,
        request_body.size() > 500 ? request_body.substr(0, 500) + "..." : request_body);

    String response = executeSearchRequest(request_body);

    const auto & header = getPort().getHeader();
    auto mutable_columns = header.cloneEmptyColumns();

    parseResponse(response, mutable_columns, header);

    if (all_read && (mutable_columns.empty() || mutable_columns[0]->empty()))
        return {};

    size_t num_rows = mutable_columns.empty() ? 0 : mutable_columns[0]->size();
    if (num_rows == 0)
        return {};

    return Chunk(std::move(mutable_columns), num_rows);
}

}
