from click.core import batch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from utils import ExperimentInfo
from utils.sql_create import Base, ExperimentInfo, ExperimentData
import pandas as pd
from utils.parse_txt_to_dataframe import parse_txt_files

class DatabaseHandler:
    def __init__(self, db_path, echo=True):
        self.engine = create_engine('sqlite:///' + db_path, echo=echo)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        # 初始化表结构（首次运行时创建）
        Base.metadata.create_all(self.engine)
    
    # write_experiment_info 输入ExperimentInfo型
    def write_experiment_info(self, info:ExperimentInfo) -> int:
        """
        写入实验信息到experiment_info表（自动检查重复数据）
        :param info: 实验信息对象
        :return: 插入后的实验ID主键（自增生成）
        """
        # 检查是否已存在相同file_name的实验信息（根据sql_create.py中file_name的unique约束）
        existing_exp = self.session.query(ExperimentInfo).filter_by(file_name=info.file_name).first()
        if existing_exp:
            raise ValueError(f"实验信息已存在，file_name: {info.file_name}")
        self.session.add(info)
        self.session.commit()
        return info.experiment_id

    # write_experiment_info 输入参数辅助方法 
    def write_experiment_info_by_params(self, file_name, prefix, date_str, time_str, batch_id): 
        """ 
        通过参数构建ExperimentInfo对象并写入表（自动检查重复数据） 
        :param file_name: 文件名（唯一） 
        :param prefix: 前缀 
        :param date_str: 日期字符串 
        :param time_str: 时间字符串 
        :param batch_id: 批次ID 
        """ 
        info = ExperimentInfo( 
            file_name=file_name, 
            prefix=prefix, 
            date_str=date_str, 
            time_str=time_str, 
            batch_id=batch_id 
        ) 
        return self.write_experiment_info(info)
    
    def write_experiment_data(self, experiment_id: int, dataframe: pd.DataFrame) -> None:
        """
        写入实验数据到experiment_data表（自动生成data_id并关联experiment_id）
        :param experiment_id: 实验ID（需存在于experiment_info表）
        :param dataframe: 包含实验数据的DataFrame，需包含ExperimentData除data_id和experiment_id外的字段：frequency, x1, y1, x2, y2
        :raises ValueError: DataFrame缺少必要字段或experiment_id不存在
        """
        # 检查experiment_id是否存在
        exp_info = self.session.query(ExperimentInfo).filter_by(experiment_id=experiment_id).first()
        if not exp_info:
            raise ValueError(f"experiment_id {experiment_id} 不存在于experiment_info表")

        # 检查DataFrame字段完整性（根据sql_create.py中ExperimentData的实际字段）
        required_columns = ['frequency', 'x1', 'y1', 'x2', 'y2']
        if not set(required_columns).issubset(dataframe.columns):
            raise ValueError(f"DataFrame缺少必要字段，至少需要：{required_columns}")

        # 遍历DataFrame行并创建ExperimentData对象
        for _, row in dataframe.iterrows():
            data_entry = ExperimentData(
                experiment_id=experiment_id,
                frequency=row['frequency'],
                x1=row['x1'],
                y1=row['y1'],
                x2=row['x2'],
                y2=row['y2']
            )
            self.session.add(data_entry)
        self.session.commit()
    
    def delete_experiment_info_by_filename(self, file_name) -> type[ExperimentInfo]:
        """
        根据文件名删除实验信息（自动检查数据是否存在）
        :param file_name: 要删除的实验信息文件名（唯一标识）
        """
        # 检查实验信息是否存在
        target_exp = self.session.query(ExperimentInfo).filter_by(file_name=file_name).first()
        if not target_exp:
            raise ValueError(f"实验信息不存在，无法删除，file_name: {file_name}")
        
        # 级联删除关联的实验数据（假设experiment_data表有外键约束且设置了级联删除，或需手动删除）
        # 若数据库外键设置了ON DELETE CASCADE，可省略此步骤；否则需手动删除关联数据
        self.session.query(ExperimentData).filter_by(experiment_id=target_exp.experiment_id).delete()
        
        # 删除实验信息
        self.session.delete(target_exp)
        self.session.commit()
        return target_exp

    # def delete_experiment_data_(self, experiment_id: int, metric: str) -> None:
    #     """
    #     根据experiment_id和metric删除实验数据（自动检查数据是否存在）
    #     :param experiment_id: 关联的实验信息ID（外键）
    #     :param metric: 指标类型（如温度、压力）
    #     :raises ValueError: 数据不存在时抛出异常
    #     """
    #     # 检查实验数据是否存在
    #     target_data = self.session.query(ExperimentData).filter_by(
    #         experiment_id=experiment_id,
    #         metric=metric
    #     ).first()
    #     if not target_data:
    #         raise ValueError(f"实验数据不存在，唯一标识: {{'experiment_id': {experiment_id}, 'metric': '{metric}'}}")
    #
    #     # 删除实验数据
    #     self.session.delete(target_data)
    #     self.session.commit()
    #
    # def get_all_experiment_info(self):
    #     """
    #     查询所有实验信息
    #     :return: 实验信息列表
    #     """
    #     return self.session.query(ExperimentInfo).all()
    #
    # def get_experiment_info_by_filename(self, file_name):
    #     """
    #     根据文件名查询实验信息
    #     :param file_name: 目标文件名
    #     :return: 实验信息对象（或None）
    #     """
    #     return self.session.query(ExperimentInfo).filter_by(file_name=file_name).first()

    def close_session(self):
        """
        关闭数据库会话
        """
        self.session.close()

if __name__ == '__main__':
    # 示例使用
    db_handler = DatabaseHandler('./experiments.db', echo=False)
    # 测试写入实验数据
    data_dict = parse_txt_files('../data')
    batch_id = 'NOG'
    for prefix, datas in data_dict.items():
        for date, data in datas.items():
            try:
                id = db_handler.write_experiment_info_by_params(
                    file_name=f'{prefix}_{date}',
                    prefix=prefix,
                    date_str=date.split('_')[0],
                    time_str=date.split('_')[1],
                    batch_id=batch_id
                )
                print('新增实验信息:', f'{prefix}_{date}')
                db_handler.write_experiment_data(
                    experiment_id=id,
                    dataframe=data
                )
                print('新增实验数据:', f'{prefix}_{date}')
            except ValueError as e:
                print("写入数据失败：", e)

    # # 测试正常删除实验数据
    # try:
    #     db_handler.delete_experiment_data(experiment_id=1, metric="temperature")
    #     print('实验数据删除成功')
    # except ValueError as e:
    #     print("删除数据失败：", e)

    # # 测试删除不存在的实验数据
    # try:
    #     db_handler.delete_experiment_data(experiment_id=1, metric="pressure")
    # except ValueError as e:
    #     print("删除数据失败（预期）：", e)

    db_handler.close_session()