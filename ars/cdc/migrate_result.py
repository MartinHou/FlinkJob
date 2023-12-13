from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common import RowKind, Row


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30000)
    env.set_parallelism(2)

    table_env = StreamTableEnvironment.create(env)

    table_env.execute_sql("""
        CREATE TABLE src_result (
            id STRING,
            input_md5 STRING,
            output_md5 STRING,
            log STRING,
            metric STRING,
            create_time TIMESTAMP(6),      
            update_time TIMESTAMP(6),
            workflow_id_id STRING,
            error_details STRING,
            error_stage STRING,
            error_type STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'mysql-cdc',
            'hostname' = '10.10.2.244',
            'port' = '3306',
            'username' = 'root',
            'password' = 'DDInfraARS123',
            'database-name' = 'ars_prod',
            'table-name' = 'result',
            'scan.startup.mode' = 'earliest-offset'
        );
    """)

    table_env.execute_sql(
        "CREATE CATALOG iceberg WITH ("
        "'type'='iceberg', "
        "'catalog-type'='hive', "
        "'uri'='thrift://100.68.81.171:9083',"
        "'warehouse'='tos://ddinfra-iceberg-test-tos/warehouse',"
        "'format-version'='2')")

    def filter_delete(x: Row):
        return x.get_row_kind().name != RowKind.DELETE.name

    change_log_table = table_env.sql_query("SELECT * FROM src_result")
    ds = table_env.to_changelog_stream(change_log_table).filter(filter_delete)
    filtered_table = table_env.from_changelog_stream(ds)

    table_env.create_temporary_view("src", filtered_table)

    table_env.execute_sql("""
                          INSERT INTO iceberg.ars.results
                          SELECT * 
                          FROM src
                          """)


if __name__ == '__main__':
    run()
