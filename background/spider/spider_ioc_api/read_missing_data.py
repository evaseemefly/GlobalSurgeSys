from typing import List

import pandas as pd
from datetime import timedelta
import io
import os
from pathlib import Path


def generate_missing_ranges(csv_file, gap_threshold_days=2, max_range_days=29):
    """
    根据缺失时间日志生成补录时间段
    :param csv_file: CSV文件路径 或 文件对象
    :param gap_threshold_days: 允许的时间间隙（天），小于此间隙将合并
    :param max_range_days: 单个时间段的最大跨度（天）
    :return: 包含补录起止时间的 DataFrame
    """
    # 1. 读取数据
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"读取数据失败: {e}")
        return pd.DataFrame()

    # 确保列名正确，清除可能存在的空格
    df.columns = df.columns.str.strip()

    # 检查必要的列是否存在
    if 'StationName' not in df.columns or 'MissingTime' not in df.columns:
        print("错误：CSV文件缺少 'StationName' 或 'MissingTime' 列")
        return pd.DataFrame()

    # 转换时间列
    df['MissingTime'] = pd.to_datetime(df['MissingTime'])

    # 结果列表
    result_ranges = []

    # 2. 按站点分组处理
    grouped = df.groupby('StationName')

    for station, group in grouped:
        # 按时间排序并去重
        times = group['MissingTime'].sort_values().unique()

        if len(times) == 0:
            continue

        # 初始化第一个片段
        start_t = times[0]
        end_t = times[0]

        for t in times[1:]:
            # 计算与当前片段起点的距离（总跨度）
            total_duration = t - start_t
            # 计算与当前片段终点的距离（间隙）
            gap = t - end_t

            # 判断是否合并：
            # 1. 合并后的总跨度必须 < max_range_days (30天限制，留余量设为29)
            # 2. 当前点与上一点的间隙 < gap_threshold_days (允许包含的非缺失段时间)
            if (total_duration < timedelta(days=max_range_days)) and \
                    (gap < timedelta(days=gap_threshold_days)):
                # 合并：更新终点
                end_t = t
            else:
                # 不合并：保存当前片段，开启新片段
                # EndTime 加 1 小时以确保覆盖最后一个缺失点
                result_ranges.append({
                    'StationName': station,
                    'StartTime': start_t,
                    'EndTime': end_t + timedelta(hours=1)
                })
                # 重置起点和终点
                start_t = t
                end_t = t

        # 保存最后一个片段
        result_ranges.append({
            'StationName': station,
            'StartTime': start_t,
            'EndTime': end_t + timedelta(hours=1)
        })

    return pd.DataFrame(result_ranges)


def merge_four_csv_files(file_paths: List[Path], output_file: Path):
    """
    将多个CSV文件合并为一个
    :param file_paths: 文件路径列表
    :param output_file: 输出文件名
    """

    all_dfs = []

    print(f"准备合并以下 {len(file_paths)} 个文件:")

    for file in file_paths:
        if not file.exists():
            print(f"⚠️ 文件不存在，跳过: {file}")
            continue

        try:
            # 读取 CSV
            # 假设文件没有表头，或者表头格式一致。
            # 如果之前的 a.csv 有表头 (StationName, MissingTime)，pandas 会自动识别
            df = pd.read_csv(str(file))

            # 简单清洗：去除列名空格
            df.columns = df.columns.str.strip()

            print(f"  - 读取 {str(file)}: {len(df)} 行")
            all_dfs.append(df)

        except Exception as e:
            print(f"❌ 读取文件 {str(file)} 失败: {e}")

    if not all_dfs:
        print("没有有效的数据被读取。")
        return

    # 合并所有 DataFrame
    merged_df = pd.concat(all_dfs, ignore_index=True)

    # 可选：去重 (如果有重复记录)
    # merged_df.drop_duplicates(inplace=True)

    # 保存结果
    merged_df.to_csv(str(output_file), index=False, encoding='utf-8-sig')

    print("-" * 30)
    print(f"✅ 合并完成！")
    print(f"总行数: {len(merged_df)}")
    print(f"结果已保存至: {str(output_file)}")


def main():
    """
    主函数：控制程序执行流程
    """
    # step1:加入前处理，合并四个csv文件为同一个文件
    root_path: Path = Path(r'/Volumes/DRCC_DATA/01DATA/03IOC/天文潮/2026/2025缺测统计/汇总')
    files_path: List[Path] = [root_path / '长期缺测_1.csv', root_path / '长期缺测_2.csv', root_path / '长期缺测_3.csv',
                              root_path / '长期缺测_4.csv']
    out_put_file: Path = root_path / '缺测_merge.csv'
    # 不需要合并新的汇聚后的数据集则不要执行此方法
    # merge_four_csv_files(files_path, out_put_file)

    # step2: 基于合并后的缺测数据集提取并声称需要爬取的时间戳集合
    # --- 1. 准备输入数据 ---
    # 这里为了演示方便，使用了字符串模拟文件。
    # 实际使用时，请将 file_path 修改为你的真实文件路径，例如: file_path = 'a.csv'
    input_filename = out_put_file

    if os.path.exists(input_filename):
        print(f"正在读取本地文件: {input_filename}")
        file_source = input_filename
    else:
        print("本地文件未找到，使用内置样例数据演示...")

    # --- 2. 执行处理逻辑 ---
    # gap_threshold_days=2: 允许中间有小于2天的非缺失数据被包含进来
    # max_range_days=29: 确保生成的请求时间段不超过30天
    df_result = generate_missing_ranges(file_source, gap_threshold_days=2, max_range_days=29)

    # --- 3. 输出结果 ---
    if not df_result.empty:
        # 格式化时间列，去掉秒后面的小数（如果有）
        df_result['StartTime'] = df_result['StartTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_result['EndTime'] = df_result['EndTime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        print("\n生成的补录时间段列表:")
        print("-" * 50)
        print(df_result.to_string(index=False))
        print("-" * 50)

        # 可选：保存到文件
        statistics_output_file = 'missing_ranges_result.csv'
        df_result.to_csv(statistics_output_file, index=False)
        print(f"\n结果已保存至: {statistics_output_file}")
    else:
        print("未生成有效的时间段，请检查输入数据。")


if __name__ == "__main__":
    main()
