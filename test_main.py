from lib import *


if __name__ == '__main__':
    # 示例：自定义批次ID映射（可选）
    # 获取解析后的嵌套字典（{前缀: {日期时间: DataFrame}}）
    data_dict = parse_txt_files('./data')
    save_data_to_database(data_dict, 'batch1')