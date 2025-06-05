from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from utils.sql_create import Base, ExperimentInfo, ExperimentData
from utils.parse_txt_to_dataframe import parse_txt_files

class DatabaseHandler:
    def __init__(self, db_path, echo=True):
        self.engine = create_engine('sqlite:///' + db_path, echo=echo)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        # 初始化表结构（首次运行时创建）
        Base.metadata.create_all(self.engine)
    
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

    def get_all_experiment_info(self) -> list[type[ExperimentInfo]]:
        """
        查询所有实验信息
        :return: 实验信息列表
        """
        return self.session.query(ExperimentInfo).all()
    
    def get_experiment_info_by_filename(self, file_name) -> type[ExperimentInfo] | None:
        """
        根据文件名查询实验信息
        :param file_name: 目标文件名
        :return: 实验信息对象（或None）
        """
        return self.session.query(ExperimentInfo).filter_by(file_name=file_name).first()

    def get_experiment_data_by_filename(self, file_name) -> pd.DataFrame:
        """
        根据文件名查询关联的实验数据
        :param file_name: 目标文件名（需存在于experiment_info表）
        :return: 实验数据列表（ExperimentData对象）
        :raises ValueError: 文件名不存在时抛出异常
        """
        # 先获取实验信息
        exp_info = self.get_experiment_info_by_filename(file_name)
        if not exp_info:
            raise ValueError(f"文件名 {file_name} 不存在于experiment_info表")
        # 查询关联的实验数据
        res = self.session.query(ExperimentData).filter_by(experiment_id=exp_info.experiment_id).all()
        # 将查询结果转换为DataFrame
        df = pd.DataFrame([(data.frequency, data.x1, data.y1, data.x2, data.y2) for data in res], columns=['frequency', 'x1', 'y1', 'x2', 'y2'])
        return df

    def update_batch_id_by_filename(self, file_name: str, batch_id_new: str) -> ExperimentInfo:
        """
        根据文件名更新实验信息中的batch_id
        :param file_name: 目标文件名（需存在于experiment_info表）
        :param batch_id_new: 新的批次ID
        :return: 更新后的实验信息对象
        :raises ValueError: 文件名不存在时抛出异常
        """
        # 获取现有实验信息
        exp_info = self.get_experiment_info_by_filename(file_name)
        if not exp_info:
            raise ValueError(f"文件名 {file_name} 不存在于experiment_info表")
        
        # 更新batch_id
        exp_info.batch_id = batch_id_new
        self.session.commit()
        return exp_info

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

    infos = db_handler.get_all_experiment_info()
    for info in infos:
        print(f'{info.file_name}:{info.batch_id}')

    batch_id_new = 'OG'
    for info in infos:
        try:
            db_handler.update_batch_id_by_filename(file_name=info.file_name, batch_id_new=batch_id_new)
            print(f'更新实验信息batch_id:{info.file_name} -> {batch_id_new}' )
        except ValueError as e:
            print("更新数据失败：", e)
    
    infos = db_handler.get_all_experiment_info()
    for info in infos:
        print(f'{info.file_name}:{info.batch_id}')

    df = db_handler.get_experiment_data_by_filename(infos[15].file_name)
    db_handler.close_session()