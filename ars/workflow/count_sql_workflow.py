import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, String, BigInteger, DateTime, JSON, Integer, FLOAT,or_
from sqlalchemy.ext.declarative import declarative_base
from common.urls import *

Base = declarative_base()


class Statistics(Base):
    __tablename__ = 'statistics'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(256), nullable=False)
    period = Column(String(128), nullable=False)
    stat_date = Column(DateTime, nullable=False)
    info = Column(JSON, nullable=False)


class Workflow(Base):
    __tablename__ = 'workflow'
    workflow_id = Column(String(32), primary_key=True)
    workflow_type = Column(String(64), nullable=False)
    workflow_name = Column(String(128), nullable=False)
    category = Column(String(64), nullable=False)
    device = Column(String(128), nullable=False)
    device_num = Column(Integer, nullable=False)

    user = Column(String(64), nullable=False)
    data_source = Column(String(128), nullable=False)
    upload_ttl = Column(FLOAT, nullable=False)
    bag_nums = Column(FLOAT, nullable=False)
    workflow_input = Column(JSON, nullable=False)
    workflow_output = Column(JSON, nullable=False)
    log = Column(JSON, nullable=False)
    workflow_status = Column(String(32), nullable=True)
    priority = Column(Integer, nullable=False)
    tag = Column(JSON, nullable=False)
    hook = Column(JSON, nullable=False)
    create_time = Column(DateTime, nullable=False)
    update_time = Column(DateTime, nullable=False)

    # foreign key
    batch_id_id = Column(String(32))

    # related tos workflow id
    tos_id = Column(String(256))

    metric = Column(JSON, nullable=False)


# 创建数据库引擎
engine = create_engine(
    'mysql+pymysql://' + MYSQL_USER + ':' + MYSQL_PASSWORD + '@' + MYSQL_HOST +
    ':3306/' + MYSQL_DATABASE,
    pool_size=100,
    max_overflow=200,
    pool_recycle=3600)

# 创建会话工厂
Session = sessionmaker(bind=engine)


class workflowActions:
    def get_workflow(self, start_time, end_time):
        # 创建会话
        session = Session()

        # 找到要获取的统计记录
        Workflows = session.query(Workflow.workflow_id, Workflow.update_time).filter(
                Workflow.update_time >= start_time,
                Workflow.update_time <= end_time+timedelta(minutes=2),
                or_(Workflow.workflow_status == "SUCCESS", Workflow.workflow_status == "FAILURE")
               )

        # 关闭会话
        session.close()

        # 返回统计记录
        return Workflows


if __name__ == "__main__":
    workflow_object = workflowActions()
    Workflows = workflow_object.get_workflow(START_TIME, END_TIME)
    workflow_id_list = []
    workflow_might_be_in_kafka = []
    for workflow in Workflows:
        if workflow.update_time >= END_TIME:
            workflow_might_be_in_kafka.append(workflow.workflow_id)
        else:
            workflow_id_list.append(workflow.workflow_id)
    print(len(workflow_id_list),len(workflow_might_be_in_kafka))
    with open(get_sql_workflow_loc(),'w') as f:
        json.dump(workflow_id_list, f, indent=4)
    with open(get_might_be_in_kafka_workflow_loc(),'w') as f:
        json.dump(workflow_might_be_in_kafka, f, indent=4)
