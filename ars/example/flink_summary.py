import logging
import sys

from typing import Iterable
import datetime

from pyflink.common import Types, WatermarkStrategy, Encoder, Time, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction, ReduceFunction, AggregateFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer, KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.common.serialization import SimpleStringSchema

from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow


class JSONTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        # 最后时间戳以毫秒为单位
        return int(value.job_start_timestamp * 1e3)


class CountAggFunction(AggregateFunction):
    """相当于count，但是window后的flink流貌似没有count，只能
       自己写一个。
       Agg可以允许accumulator和add不一样，但是reduce必须是一样的。
       所以Agg灵活性更高。
    """

    def create_accumulator(self) -> int:
        return 0

    def add(self, value: tuple, accumulator: int) -> int:
        return accumulator + 1

    def get_result(self, accumulator) -> int:
        return accumulator

    def merge(self, a: int, b: int) -> tuple:
        return a + b


class AddMetaProcessFunction(
        ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    """一般用ProcessFunction对accumulator的结果进行处理，可以额外填入一些辅助信息。
       如这个例子里，窗口起始时间
       在和agg混用的情况下，elements里实际只有一个元素，就是agg的结果
    """

    def process(self, key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        count = elements[0]
        return (datetime.datetime.fromtimestamp(context.window().start * 1e-3),
                key, count)


def read_from_kafka(env):
    SCHEMA = {
        "pod_id": Types.STRING(),
        "status": Types.STRING(),
        "job_start_timestamp": Types.FLOAT(),
        "job_finish_timestamp": Types.FLOAT(),
        "device": Types.STRING(),
        "group": Types.STRING(),
        "reason": Types.STRING(),
        "log": Types.STRING(),
    }

    KEYS = [k for k in SCHEMA.keys()]
    VALUES = [SCHEMA[k] for k in KEYS]
    # 嵌套的schema暂时没试，todo。
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW_NAMED(KEYS, VALUES)) \
        .build()
    kafka_consumer = FlinkKafkaConsumer(
        topics='test_flink_with_kafka',
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers':
            '10.10.2.224:9092,10.10.2.81:9092,10.10.3.141:9092',
            'group.id':
            'pyflink-test',
        })

    # 也可以使用set_start_from_timestamp
    kafka_consumer.set_start_from_earliest()

    return kafka_consumer


def analyse(env: StreamExecutionEnvironment, kafka_source):
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_hours(2)) \
        .with_timestamp_assigner(JSONTimestampAssigner())

    stream = env.add_source(kafka_source)
    result1 = stream \
       .filter(lambda x: x.status != 'SUCCESS') \
       .assign_timestamps_and_watermarks(watermark_strategy) \
       .key_by(lambda x: x.status) \
       .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8))) \
       .aggregate(CountAggFunction(), window_function=AddMetaProcessFunction())

    def log_analyser(x):
        if 'A' in x.log:
            tag = 'A'
        elif 'B' in x.log:
            tag = 'B'
        elif 'C' in x.log:
            tag = 'C'
        else:
            tag = 'D'
        return x, tag

    result2 = stream \
       .map(lambda x: log_analyser(x))

    result1.print()
    result2.print()

    env.execute()


if __name__ == '__main__':
    logging.basicConfig(
        stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(
        "file:///mnt/data/yu.gao/repos/flink-jobs/flink-sql-connector-kafka-1.15.4.jar"
    )

    source = read_from_kafka(env)
    analyse(env, source)