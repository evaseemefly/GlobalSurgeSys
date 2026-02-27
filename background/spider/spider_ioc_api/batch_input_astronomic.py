import os
import re
from typing import List
import pandas as pd
import arrow
from sqlalchemy import text
from core.db import DbFactory  # 请确保这里 DbFactory 的导入路径与你的项目结构一致


def check_astronomic_tide_codes(result_codes) -> List[str]:
    """
    检查 result_codes 中的 code 是否在 station_astronomic_tide 表中存在指定时间范围内的记录
    """
    if not result_codes:
        print("传入的 code 列表为空！")
        return []

    db_factory = DbFactory()

    start_time = '2026-01-01 00:00:00'
    end_time = '2026-12-31 23:00:00'

    # 将 Python 列表转换为 SQL IN 语句需要的字符串格式，例如: 'abas', 'hoko', 'holm'
    codes_str = ", ".join([f"'{code}'" for code in result_codes])

    print(f"开始在 station_astronomic_tide 表中检索 {len(result_codes)} 个站点...")

    # 构建原生 SQL：使用 DISTINCT 避免查出大量重复数据
    sql = f"""
        SELECT DISTINCT station_code 
        FROM station_astronomic_tide 
        WHERE station_code IN ({codes_str}) 
          AND gmt_realtime >= :start_time 
          AND gmt_realtime <= :end_time
    """

    exist_codes = set()
    not_exist_codes: List[str] = []

    try:
        # 建立连接并执行查询
        with db_factory.engine.connect() as conn:
            result = conn.execute(text(sql), {"start_time": start_time, "end_time": end_time})

            # 遍历查询结果，将存在的 code 加入集合
            for row in result:
                exist_codes.add(row[0])  # row[0] 是 station_code

        # 计算不存在的 code (总集合 - 存在的集合)
        not_exist_codes = list(set(result_codes) - exist_codes)

        # 存入数组并排序，使输出更加美观
        exist_codes_list = sorted(list(exist_codes))
        not_exist_codes = sorted(not_exist_codes)

        print(f"\n================ 检索完成 ================")
        print(f"--- 存在的 Code (共 {len(exist_codes_list)} 个) ---")
        print(exist_codes_list)

        print(f"\n--- 不存在的 Code (共 {len(not_exist_codes)} 个) ---")
        print(not_exist_codes)
        return not_exist_codes

    except Exception as e:
        print(f"数据库查询异常: {e}")
        return []


def extract_codes_from_directory(directory_path):
    """
    提取指定目录下所有文件名中第一个 '_' 之前的 code
    要求 code 必须是英文或英文+数字的组合
    """
    codes = set()  # 使用 set 去重

    # 检查目录是否存在
    if not os.path.exists(directory_path):
        print(f"错误: 目录 '{directory_path}' 不存在！")
        return []

    # 编译正则表达式：仅匹配字母和数字组合 (英文或英文+数字)
    # ^[a-zA-Z0-9]+$ 表示从头到尾都必须是字母或数字
    pattern = re.compile(r'^[a-zA-Z0-9]+$')

    # 遍历目录下的所有文件
    for filename in os.listdir(directory_path):
        # 跳过隐藏文件 (如 .DS_Store) 或目录
        if filename.startswith('.') or os.path.isdir(os.path.join(directory_path, filename)):
            continue

        # 确保文件名中包含 '_'
        if '_' in filename:
            # 提取第一个 '_' 之前的部分
            code = filename.split('_')[0]

            # 校验提取出的 code 是否符合"英文或英文+数字"的要求
            if pattern.match(code):
                codes.add(code)

    # 转换为列表并排序，方便查看
    return sorted(list(codes))


def batch_insert_astronomic_tides(codes: List[str], data_dir: str, file_name_stamp: str = '_2026_Prediction.txt'):
    """
    将缺失站点的天文潮数据批量录入数据库 (基于 arrow 时间处理)
    """
    if not codes:
        print("没有需要入库的站点 code。")
        return

    db_factory = DbFactory()

    print(f"准备为 {len(codes)} 个站点入库数据...")

    # 准备插入的 SQL 模板
    insert_sql = text("""
        INSERT INTO surge_global_sys.station_astronomic_tide 
        (station_code, gmt_realtime, surge, gmt_create_time, gmt_modify_time, is_del, ts)
        VALUES (:station_code, :gmt_realtime, :surge, :gmt_create_time, :gmt_modify_time, :is_del, :ts)
    """)

    # 建立数据库连接
    with db_factory.engine.connect() as conn:
        for code in codes:
            # 1. 构造文件路径
            filename = f"{code}{file_name_stamp}"  # 根据你的实际后缀修改，如 .csv 或 .txt
            filepath = os.path.join(data_dir, filename)

            if not os.path.exists(filepath):
                print(f"⚠️ 找不到站点 {code} 的数据文件: {filepath}，跳过该站点。")
                continue

            print(f"开始处理站点 {code} 的数据...")

            try:
                # 2. 读取文件并解析
                df = pd.read_csv(filepath, sep=r'\s+', header=None, names=['date', 'time', 'surge'])

                records = []

                # 获取当前 UTC 时间，并直接格式化为标准的 MySQL DateTime 字符串格式
                now_str = arrow.utcnow().format('YYYY-MM-DD HH:mm:ss')

                # 3. 构造批量插入的数据字典列表
                for _, row in df.iterrows():
                    # 将日期和时间拼在一起，例如 "2026-01-01 00:00"
                    time_str = f"{row['date']} {row['time']}"

                    # 使用 arrow 直接解析字符串，并显式指定其为 UTC 时区
                    arr_time = arrow.get(time_str, 'YYYY-MM-DD HH:mm').replace(tzinfo='UTC')

                    records.append({
                        'station_code': code,
                        # 格式化为 YYYY-MM-DD HH:mm:ss 存入 MySQL 的 datetime 字段
                        'gmt_realtime': arr_time.format('YYYY-MM-DD HH:mm:ss'),
                        'surge': float(row['surge']),
                        'gmt_create_time': now_str,
                        'gmt_modify_time': now_str,
                        'is_del': 0,
                        # arrow 库直接提供了 int_timestamp 属性，免去了原来的强转和浮点数转换
                        'ts': arr_time.int_timestamp
                    })

                # 4. 执行批量插入
                if records:
                    conn.execute(insert_sql, records)
                    conn.commit()
                    print(f"✅ 站点 {code} 成功入库 {len(records)} 条记录。")
                else:
                    print(f"⚠️ 站点 {code} 文件中没有解析到有效数据。")

            except Exception as e:
                # 如果发生异常，回滚当前站点的操作
                conn.rollback()
                print(f"❌ 处理站点 {code} 时发生错误: {e}")

    print("================ 批量入库任务结束 ================")


def extract_tide_from_dir():
    pass


def main():
    # step1: 获取当前已有的天文潮code集合
    target_dir = "/Volumes/DRCC_DATA/01DATA/03IOC/天文潮/2026/2026_m"
    result_codes = extract_codes_from_directory(target_dir)
    print(f"总共提取到 {len(result_codes)} 个独立的 code。")
    print("-" * 40)
    # 打印结果，也可以将结果写入到文本文件中
    print(result_codes)

    # step2: 判断数据库中已有的code和不存在的code
    # TODO: 把你提取出来的完整 result_codes 数组粘贴在这里
    not_exist_codes: List[str] = check_astronomic_tide_codes(result_codes)

    # step3: 将不存在的code集合遍历并录入数据库
    batch_insert_astronomic_tides(not_exist_codes, target_dir)

    pass


if __name__ == '__main__':
    main()
