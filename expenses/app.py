"""Expense Analytics dashboard — Flask application factory.

Structure:
  services/     data + domain logic (expenses, gold, photos)
  blueprints/   HTTP route handlers grouped by domain
  static/       css + js assets
  templates/    dashboard.html skeleton
"""
import os

from flask import Flask, jsonify, render_template, request, url_for

from blueprints.expenses_api import bp as expenses_bp
from blueprints.gold_api import bp as gold_bp
from blueprints.photos_api import bp as photos_bp

# Cap upload/request bodies so a huge (or hostile) POST can't exhaust disk/memory
# before we ever parse it. Expense CSVs are tiny (a year of transactions is well
# under 1 MB); 8 MB is generous headroom. Override with $EXPENSES_MAX_UPLOAD_MB.
try:
    _MAX_UPLOAD_MB = float(os.environ.get('EXPENSES_MAX_UPLOAD_MB', '8'))
except ValueError:
    _MAX_UPLOAD_MB = 8
MAX_CONTENT_LENGTH = int(_MAX_UPLOAD_MB * 1024 * 1024)


def create_app():
    app = Flask(__name__)
    app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH
    # Don't let the browser hold onto stale CSS/JS: serve static files with a 0s cache
    # so edits show up on the next load. Combined with the ?v=<mtime> cache-bust below,
    # this guarantees the page never renders old assets against new HTML.
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    # Re-read templates from disk each request so an edit to dashboard.html shows up on
    # the next reload without a manual server restart (local single-user app).
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.jinja_env.auto_reload = True

    # The dashboard and its API are same-origin (the browser loads both from this
    # server via relative URLs), so we deliberately do NOT enable permissive CORS.
    # Reflecting arbitrary origins would let any website the user is visiting read
    # their transactions and call the destructive report/settings endpoints.

    app.register_blueprint(expenses_bp)
    app.register_blueprint(gold_bp)
    app.register_blueprint(photos_bp)

    @app.context_processor
    def _asset_helper():
        # `static_url('css/foo.css')` → /static/css/foo.css?v=<mtime>, so a changed file
        # always gets a fresh URL and the browser can't serve a cached older copy.
        def static_url(filename):
            path = os.path.join(app.static_folder, filename)
            try:
                ver = int(os.path.getmtime(path))
            except OSError:
                ver = 0
            return url_for('static', filename=filename, v=ver)
        return {'static_url': static_url}

    @app.route('/')
    def dashboard():
        return render_template('dashboard.html')

    @app.after_request
    def _no_store_static(resp):
        # This is a local single-user dev app: never let the browser cache our own
        # CSS/JS, so an edit always shows up on the next reload (no stale ES modules
        # rendering old layout against new HTML). Only our static assets — not photos.
        if request.path.startswith('/static/') and not request.path.startswith('/static/photo'):
            resp.headers['Cache-Control'] = 'no-store, max-age=0'
        return resp

    @app.errorhandler(413)
    def _too_large(_e):
        limit_mb = app.config['MAX_CONTENT_LENGTH'] / (1024 * 1024)
        return jsonify({'error': f'File too large (limit {limit_mb:.0f} MB).'}), 413

    return app


app = create_app()


if __name__ == '__main__':
    # debug/reloader off by default; opt in with FLASK_DEBUG=1 for local dev.
    app.run(debug=os.environ.get('FLASK_DEBUG') == '1', port=5001)
