"""冰码头预约取冰服务（零依赖，仅 Python 标准库）。

用法:
    python -m ice_dock [--host 127.0.0.1] [--port 8765] [--db ice_dock.db]
"""
import argparse

from .db import init_db
from .server import serve


def main():
    ap = argparse.ArgumentParser(
        prog="ice_dock",
        description="冰码头预约取冰服务（零依赖，仅 Python 标准库）",
    )
    ap.add_argument("--host", default="127.0.0.1", help="监听地址")
    ap.add_argument("--port", type=int, default=8765, help="监听端口")
    ap.add_argument("--db", default="ice_dock.db", help="数据库文件路径")
    args = ap.parse_args()

    init_db(args.db)
    serve(args.db, args.host, args.port)


if __name__ == "__main__":
    main()
