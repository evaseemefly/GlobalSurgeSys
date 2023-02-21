# 这是一个示例 Python 脚本。

# 按 Ctrl+F5 执行或将其替换为您的代码。
# 按 按两次 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
from models.models import to_migrate


def print_hi(name):
    # 在下面的代码行中使用断点来调试脚本。
    print(f'Hi, {name}')  # 按 F9 切换断点。


def to_db():
    to_migrate()
    pass


def main():
    to_db()


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    main()
# print_hi('PyCharm')

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
