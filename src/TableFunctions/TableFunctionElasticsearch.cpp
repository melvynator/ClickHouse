#include <Storages/StorageElasticsearch.h>

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

class TableFunctionElasticsearch : public ITableFunction
{
public:
    static constexpr auto name = "elasticsearch";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context,
        const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "Elasticsearch"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ElasticsearchConfiguration configuration;
};

StoragePtr TableFunctionElasticsearch::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto storage = std::make_shared<StorageElasticsearch>(
        StorageID(getDatabaseName(), table_name),
        configuration,
        columns,
        ConstraintsDescription(),
        String{});
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionElasticsearch::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return parseColumnsListFromString(configuration.structure, context);
}

void TableFunctionElasticsearch::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function elasticsearch must have arguments.");

    ASTs & args = func_args.arguments->children;
    configuration = StorageElasticsearch::getConfiguration(args, context);
}

}

void registerTableFunctionElasticsearch(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionElasticsearch>(
    {
        .description = "Reads data from an Elasticsearch 8.x+ index via the search API with search_after pagination.",
        .examples = {{"basic", "SELECT * FROM elasticsearch(host, idx, user, pass, structure)", ""}},
        .category = FunctionDocumentation::Category::TableFunction
    });
}

}
