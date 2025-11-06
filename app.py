from flask import Flask
from crawling.api import crawl_bp

def create_app():
    app = Flask(__name__)
    app.register_blueprint(crawl_bp)
    return app

if __name__ == '__main__':
    create_app().run(host='0.0.0.0', port=5001)
