# 这是一个示例 Python 脚本。

# 按 Ctrl+F5 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
from fastapi import FastAPI
import uvicorn

# 项目文件
from application import urls


def init_app():
    app = FastAPI(
        title="Kinit",
        description="本项目基于Fastapi与Vue3+Typescript+Vite4+element-plus的基础项目 前端基于vue-element-plus-admin框架开发",
        version="1.0.0"
    )

    for url in urls.urlpatterns:
        app.include_router(url["ApiRouter"], prefix=url["prefix"], tags=url["tags"])
    return app


@shell_app.command()
def run():
    """
    启动项目
    """
    uvicorn.run(app='main:init_app', host="0.0.0.0", port=9000)


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    print_hi('PyCharm')

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
