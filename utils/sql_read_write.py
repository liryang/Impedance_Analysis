from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sql_create import ExperimentInfo  # 导入数据库模型类

# 定义数据库连接
engine = create_engine("sqlite:///experiments.db")

def save_data_to_database(data_dict, batch_id=None):
    # 创建数据库会话
    Session = sessionmaker(bind=engine)
    session = Session()

    # 遍历所有文件数据
    for prefix, datetime_dict in data_dict.items():
        for datetime_str, df in datetime_dict.items():
            # 构造原始文件名（示例：Organoid1_20241201_134610.txt）
            file_name = f"{prefix}_{datetime_str}.txt"
            date_str = datetime_str[:8]  # 提取日期（前8位）
            time_str = datetime_str[8:]   # 提取时间（后6位）

            # 适配test_main调用的批次ID处理逻辑（支持字符串/字典）
            if isinstance(batch_id, dict):
                current_batch_id = batch_id.get(prefix, prefix)  # 字典类型按前缀匹配
            elif isinstance(batch_id, str):
                current_batch_id = batch_id  # 字符串类型直接使用
            else:
                current_batch_id = prefix  # 默认使用数据前缀作为批次ID

            # 写入实验信息表（确保文件名唯一）
            experiment = ExperimentInfo(
                file_name=file_name,
                prefix=prefix,
                date_str=date_str,
                time_str=time_str,
                batch_id=current_batch_id  # 使用处理后的current_batch_id
            )
            session.add(experiment)
            session.flush()  # 获取自动生成的experiment_id

            # 处理DataFrame列名（匹配数据库字段名）
            df_renamed = df.rename(columns={
                'fre': 'frequency',
                'X1': 'x1',
                'Y1': 'y1',
                'X2': 'x2',
                'Y2': 'y2'
            })

            # 关联experiment_id并写入数据表
            df_renamed['experiment_id'] = experiment.experiment_id
            df_renamed.to_sql(
                name="experiment_data",
                con=engine,
                if_exists="append",
                index=False
            )

    # 提交事务并关闭会话
    session.commit()
    session.close()
    print("数据已成功保存到数据库")