import os
import pandas as pd

def parse_txt_files(data_dir: str):
    # 初始化结果字典（嵌套结构：{前缀: {日期时间: DataFrame}}）
    result_dict = {}
    
    # 遍历data目录下的所有txt文件
    for filename in os.listdir(data_dir):
        if filename.endswith('.txt'):
            # 提取文件名前缀和日期时间部分（分割第一个下划线）
            parts = filename.split('_', 1)
            if len(parts) < 2:
                continue  # 跳过格式不正确的文件名
            prefix, rest = parts
            datetime_key = rest[:-4]  # 去掉.txt扩展名获取日期时间部分
            file_path = os.path.join(data_dir, filename)
            
            try:
                # 读取txt文件为DataFrame
                df = pd.read_csv(file_path, sep=';', comment='%', engine='python')  # 使用分号分隔，自动跳过以%开头的注释行
                df.columns = ['fre', 'X1', 'Y1', 'X2', 'Y2']
                
                # 按前缀分类存储为嵌套字典
                if prefix not in result_dict:
                    result_dict[prefix] = {}
                result_dict[prefix][datetime_key] = df  # 以日期时间为子键存储DataFrame
            
            except Exception as e:
                print(f'读取文件 {filename} 失败: {str(e)}')
    
    return result_dict

if __name__ == '__main__':
    # 执行解析并生成字典
    dataframe_dict = parse_txt_files(data_dir='./data')
    print()
    