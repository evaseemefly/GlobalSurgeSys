import os
from typing import Any

from pydantic import Field, BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# ------------------- 1. 使用 Pydantic 模型定义配置结构 -------------------

class DefaultDbSettings(BaseModel):
    """MySQL 数据库配置模型"""
    ENGINE: str = 'pymysql'
    NAME: str
    USER: str
    PASSWORD: str
    HOST: str
    PORT: int

    # Pydantic 会自动查找以 'DB_' 开头的环境变量
    # 例如，这里的 HOST 字段会对应到 DB_HOST 环境变量
    # model_config = SettingsConfigDict(env_prefix='DB_')


class MongoDbSettings(BaseModel):
    """MongoDB 配置模型"""
    NAME: str
    USER: str
    PASSWORD: str
    HOST: str
    PORT: int

    # model_config = SettingsConfigDict(env_prefix='MONGO_')


# 然后定义包含 'gts' 的外层结构
# class StoreOptionsSettings(BaseModel):
#     # gts: GtsStoreSettings = Field(default_factory=GtsStoreSettings)
#     # path: str = '/data/share'
#     # relative_path: str = 'Decoded_Data'
#     path: str
#     relative_path: str

class StoreOptionsSettings(BaseModel):
    path: str
    relative_path: str


class TaskSettings(BaseSettings):
    """任务配置模型"""
    name_prefix: str = Field('TASK_SPIDER_GLOBAL_', alias='TASK_NAME_PREFIX')
    interval: int = Field(10, alias='TASK_INTERVAL')  # 单位min


class DbTableSplitSettings(BaseSettings):
    """分表配置模型"""
    tab_split_name: str = 'station_realdata_specific'


# --- 新增部分：为 STORE_OPTIONS 定义模型 ---
class GtsStoreSettings(BaseModel):
    path: str = '/data/share'
    relative_path: str = 'Decoded_Data'


class AllSettings(BaseSettings):
    """
    所有配置的统一入口
    它会自动从 .env 文件和环境变量中加载配置
    """
    # model_config 指示 Pydantic 从哪里以及如何加载配置
    # 1. env_file: 指定 .env 文件的路径
    # 2. env_file_encoding: 确保正确读取中文字符
    # 3. extra='ignore': 忽略 .env 文件中未在模型里定义的额外变量
    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'),
        env_file_encoding='utf-8',
        extra='ignore',
        # 新增：告诉 Pydantic 字段名是区分大小写的（这是默认行为，但明确写出更好）
        case_sensitive=True,
        # 新增：定义环境变量的嵌套分隔符，例如 STORE_OPTIONS__GTS__PATH
        nested_env_delimiter='__'
    )

    # 嵌套配置模型
    # 不要创建默认实例，只声明类型
    # Pydantic 会在加载时自动处理
    # db: DefaultDbSettings
    # mongo: MongoDbSettings
    # task_options: TaskSettings
    # station_split_options: DbTableSplitSettings
    # ---------
    # db: DefaultDbSettings = DefaultDbSettings()
    # mongo: MongoDbSettings = MongoDbSettings()
    # task_options: TaskSettings = TaskSettings()
    # station_split_options: DbTableSplitSettings = DbTableSplitSettings()
    # ----------
    # --- 已修正 ---
    # 使用 Field(default_factory=...) 来推迟嵌套模型的实例化。
    # 这样，当 DefaultDbSettings() 被调用时，AllSettings 已经加载了 .env 文件的内容。
    # db: DefaultDbSettings = Field(default_factory=DefaultDbSettings)
    # mongo: MongoDbSettings = Field(default_factory=MongoDbSettings)
    # task_options: TaskSettings = Field(default_factory=TaskSettings)
    # station_split_options: DbTableSplitSettings = Field(default_factory=DbTableSplitSettings)

    # ------
    # --- 终极解决方案 ---
    # 在父模型的字段上使用 env_prefix 来收集和填充子模型
    # 这会告诉 AllSettings:
    # "去环境变量里找所有以 'DB_' 开头的变量，
    #  去掉这个前缀，然后用剩下的键值对来填充一个 DefaultDbSettings 对象，
    #  最后把这个对象赋值给 'db' 字段。"
    db: DefaultDbSettings
    mongo: MongoDbSettings
    # --- 新增部分：将 StoreOptionsSettings 集成到主配置中 ---
    store_options: StoreOptionsSettings

    # 对于没有前缀或有别名的字段，保持原样或使用 default_factory
    task_options: TaskSettings = Field(default_factory=TaskSettings)
    station_split_options: DbTableSplitSettings = Field(default_factory=DbTableSplitSettings)

    @model_validator(mode='before')
    @classmethod
    def preprocess_nested_settings(cls, data: Any) -> Any:
        """
        在 Pydantic 验证之前，手动将扁平的环境变量分组为嵌套字典。
        """
        if isinstance(data, dict):
            # 1. 筛选所有以 'DB_' 开头的键值对，去掉前缀，并转为小写，创建一个新字典
            db_data = {
                key.replace('DB_', '', 1): value
                for key, value in data.items() if key.startswith('DB_')
            }
            # 2. 筛选所有以 'MONGO_' 开头的键值对
            mongo_data = {
                key.replace('MONGO_', '', 1): value
                for key, value in data.items() if key.startswith('MONGO_')
            }
            # 3. 筛选存储相关的配置信息
            store_data = {
                key.replace('STORE_', '', 1).lower(): value
                for key, value in data.items() if key.startswith('STORE_')
            }

            # 3. 如果找到了数据，就将其作为嵌套字典赋值给 'db' 和 'mongo' 键
            #    Pydantic 稍后会用这个字典来创建 DefaultDbSettings 实例
            if db_data:
                data['db'] = db_data
            if mongo_data:
                data['mongo'] = mongo_data
            if store_data:
                data['store_options'] = store_data

        return data


# ------------------- 2. 实例化配置对象，全局唯一 -------------------

# 创建一个全局的配置实例，项目中其他地方都从这里导入
settings = AllSettings()

# ------------------- 3. (可选) 兼容旧代码，导出旧格式的变量 -------------------

# 为了让项目中其他使用 DATABASES 的旧代码不用修改，我们可以重新组装这个变量
# .model_dump() 方法可以将 Pydantic 模型转换为字典
DATABASES = {
    'default': {
        **settings.db.model_dump(),  # model_dump() 会将模型实例转为字典
        'OPTIONS': {
            "init_command": "SET foreign_key_checks = 0;",
        },
    },
    'mongo': settings.mongo.model_dump()
}

TASK_OPTIONS = settings.task_options.model_dump()

DB_TABLE_SPLIT_OPTIONS = {
    'station': settings.station_split_options.model_dump()
}

# --- 新增部分：导出 STORE_OPTIONS 字典 ---
STORE_OPTIONS = settings.store_options.model_dump()
pass

