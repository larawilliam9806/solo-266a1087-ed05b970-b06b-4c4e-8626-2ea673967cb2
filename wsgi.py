"""生产 WSGI 入口：gunicorn wsgi:app。"""

from app import create_app

app = create_app()

if __name__ == "__main__":
    # 无 gunicorn 时的本地直跑方式
    app.run(host="0.0.0.0", port=8000)
