import json
import os
from pathlib import Path

SOURCE_PATH: Path = Path(r'/Volumes/DRCC_DATA/01DATA/03全球监测系统/api_v2/通过api获取的数据')


def process_station_data(file_path: Path, source_path: Path):
    # 1. 读取 JSON 文件
    # 如果你的文件名不同，请修改这里的路径
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data_list = json.load(f)
    except FileNotFoundError:
        print(f"错误: 找不到文件 {file_path}")
        return

    # 用于去重的字典和集合
    country_map = {}  # 使用字典以 country 代码为 key 进行去重
    location_set = set()  # 使用集合对 location 进行去重

    # 2. 遍历数据
    for item in data_list:
        # --- 处理国家 ---
        c_code = item.get("country")
        c_name = item.get("countryname")

        # 只有当国家代码存在且不在字典中时才添加（去重）
        if c_code and c_code not in country_map:
            country_map[c_code] = {
                "country": c_code,
                "countryname": c_name
            }

        # --- 处理地点 ---
        loc = item.get("Location")
        if loc:
            location_set.add(loc)

    # 3. 生成最终结果列表
    unique_countries = list(country_map.values())
    unique_locations = list(location_set)
    # 对地点排序，方便查看（可选）
    unique_locations.sort()

    # 4. 写入 country.json
    with open(str(source_path / 'country.json'), 'w', encoding='utf-8') as f:
        json.dump(unique_countries, f, ensure_ascii=False, indent=4)

    # 5. 写入 location.json
    with open(str(source_path / 'location.json'), 'w', encoding='utf-8') as f:
        json.dump(unique_locations, f, ensure_ascii=False, indent=4)

    # 6. 打印国家数量
    print(f"处理完成。")
    print(f"国家总数 (去重后): {len(unique_countries)}")
    print(f"地点总数 (去重后): {len(unique_locations)}")
    print(f"结果已保存在 'country.json' 和 'location.json'")


if __name__ == "__main__":
    # 为了演示，创建一个临时的源文件（你可以直接指定你现有的文件路径）
    source_filename = str(SOURCE_PATH / "stations.json")

    # 假设这是你的现有文件路径，请修改为实际路径
    if os.path.exists(source_filename):
        process_station_data(source_filename, SOURCE_PATH)
    else:
        # 如果文件不存在，这里创建一个样例文件用于测试代码
        sample_data = [
            {
                "Code": "aarh", "Location": "Aarhus", "country": "DMK", "countryname": "Denmark",
                "lastvalue": 0.28
            },
            {
                "Code": "test", "Location": "Aarhus", "country": "DMK", "countryname": "Denmark",
                "lastvalue": 0.29
            },
            {
                "Code": "koto", "Location": "Koto", "country": "JPN", "countryname": "Japan",
                "lastvalue": 1.1
            }
        ]
        with open(source_filename, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f)

        print(f"已创建测试文件 {source_filename}")
        process_station_data(source_filename, SOURCE_PATH)
