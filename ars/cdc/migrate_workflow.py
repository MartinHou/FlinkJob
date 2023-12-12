from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common import RowKind, Row


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(5000)
    env.set_parallelism(1)
    
    table_env = StreamTableEnvironment.create(env)
    
    table_env.execute_sql("""
        CREATE TABLE src_workflow (
            workflow_id STRING,
            workflow_type STRING, 
            workflow_name STRING, 
            `user` STRING, 
            workflow_input STRING, 
            workflow_output STRING, 
            log STRING, 
            workflow_status STRING, 
            priority INT, 
            tag  STRING, 
            create_time TIMESTAMP(6), 
            update_time TIMESTAMP(6), 
            batch_id_id STRING, 
            hook STRING, 
            device STRING, 
            tos_id STRING, 
            device_num INT, 
            data_source STRING, 
            category STRING, 
            upload_ttl DOUBLE, 
            bag_nums INT, 
            metric STRING,
            PRIMARY KEY (workflow_id) NOT ENFORCED
        ) WITH (
            'connector' = 'mysql-cdc',
            'hostname' = '10.10.2.244',
            'port' = '3306',
            'username' = 'root',
            'password' = 'DDInfraARS123',
            'database-name' = 'ars_prod',
            'table-name' = 'workflow',
            'scan.incremental.snapshot.chunk.size' = '512',
            'scan.snapshot.fetch.size' = '512',
            'scan.startup.mode' = 'earliest-offset'
        );
    """)
    
    table_env.execute_sql("CREATE CATALOG iceberg WITH ("
                      "'type'='iceberg', "
                      "'catalog-type'='hive', "
                      "'uri'='thrift://100.68.81.171:9083',"
                      "'warehouse'='tos://ddinfra-iceberg-test-tos/warehouse',"
                      "'format-version'='2')")
    
    def filter_delete(x:Row):
        return x.get_row_kind().name!=RowKind.DELETE.name
        
    change_log_table = table_env.sql_query("SELECT * FROM src_workflow")
    ds = table_env.to_changelog_stream(change_log_table).filter(filter_delete)
    filtered_table = table_env.from_changelog_stream(ds)
    
    table_env.create_temporary_view("src", filtered_table)
    
    table_env.execute_sql("""
                          INSERT INTO iceberg.ars.workflows
                          SELECT 
                            workflow_id,
                            workflow_type, 
                            workflow_name, 
                            `user`, 
                            workflow_input, 
                            workflow_output, 
                            log, 
                            workflow_status, 
                            priority, 
                            tag _tag, 
                            create_time, 
                            update_time, 
                            batch_id_id, 
                            hook, 
                            device, 
                            tos_id, 
                            device_num, 
                            data_source, 
                            category, 
                            CAST(ROUND(upload_ttl) AS INT), 
                            bag_nums, 
                            metric
                          FROM src
                          """)
    
    
if __name__=='__main__':
    run()