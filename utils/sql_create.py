from sqlalchemy import create_engine, Column, Integer, Text, REAL, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker

# 定义数据库基类
Base = declarative_base()

# 实验信息表
class ExperimentInfo(Base):
    __tablename__ = "experiment_info"
    experiment_id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(Text, unique=True, nullable=False)  # 确保文件名唯一
    prefix = Column(Text)
    date_str = Column(Text)
    time_str = Column(Text)
    batch_id = Column(Text)  # 手动指定的批次ID

# 实验数据表
class ExperimentData(Base):
    __tablename__ = "experiment_data"
    data_id = Column(Integer, primary_key=True, autoincrement=True)
    experiment_id = Column(Integer, ForeignKey("experiment_info.experiment_id"))
    frequency = Column(REAL)
    x1 = Column(REAL)
    y1 = Column(REAL)
    x2 = Column(REAL)
    y2 = Column(REAL)


if __name__ == "__main__":
    # 创建数据库和表（如果不存在）
    engine = create_engine("sqlite:///experiments.db")
    Base.metadata.create_all(engine)