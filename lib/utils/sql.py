from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, String, BigInteger, DateTime, JSON, Integer, FLOAT
from sqlalchemy.ext.declarative import declarative_base

MYSQL_USER = 'ars_dev'
MYSQL_PASSWORD = '01234567'
MYSQL_HOST = '10.8.106.103'
MYSQL_DATABASE = 'ars_local'
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
    batch_id = Column(String(32))

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


class StatisticsActions:
    def add_statistics(self, name, period, stat_date, info):
        # 创建会话
        session = Session()

        # 创建新统计记录
        new_statistics = Statistics(
            name=name, period=period, stat_date=stat_date, info=info)

        # 添加新统计记录
        session.add(new_statistics)

        # 提交事务
        session.commit()

        # 关闭会话
        session.close()

    def update_statistics(self, name, period, stat_date, info):
        # 创建会话
        session = Session()

        # 找到要更新的统计记录
        statistics = session.query(Statistics).filter_by(
            name=name, period=period, stat_date=stat_date).first()

        # 更新统计记录信息
        if info:
            statistics.info = info

        # 提交事务
        session.commit()

        # 关闭会话
        session.close()

    def delete_statistics(self, statistics_id):
        # 创建会话
        session = Session()

        # 找到要删除的统计记录
        statistics = session.query(Statistics).filter_by(
            id=statistics_id).first()

        # 删除统计记录
        session.delete(statistics)

        # 提交事务
        session.commit()

        # 关闭会话
        session.close()

    def get_statistics(self, name, period, stat_date):
        # 创建会话
        session = Session()

        # 找到要获取的统计记录
        statistics = session.query(Statistics).filter_by(
            name=name, period=period, stat_date=stat_date)

        # 关闭会话
        session.close()

        # 返回统计记录
        return statistics


if __name__ == "__main__":
    stati = StatisticsActions()
    stati.add_statistics(
        name='stat_failure_pod_group_by_type',
        period='daily',
        stat_date='2023-07-03 00:00:00',
        info={})
