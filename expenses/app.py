"""Expense Analytics dashboard — Flask application factory.

Structure:
  services/     data + domain logic (expenses, gold, photos)
  blueprints/   HTTP route handlers grouped by domain
  static/       css + js assets
  templates/    dashboard.html skeleton
"""
from flask import Flask, render_template
from flask_cors import CORS

from blueprints.expenses_api import bp as expenses_bp
from blueprints.gold_api import bp as gold_bp
from blueprints.photos_api import bp as photos_bp


def create_app():
    app = Flask(__name__)
    CORS(app)

    app.register_blueprint(expenses_bp)
    app.register_blueprint(gold_bp)
    app.register_blueprint(photos_bp)

    @app.route('/')
    def dashboard():
        return render_template('dashboard.html')

    return app


app = create_app()


if __name__ == '__main__':
    app.run(debug=True, port=5001)
