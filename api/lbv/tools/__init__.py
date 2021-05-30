from flask import Blueprint, render_template, abort
from jinja2 import TemplateNotFound

tools_bp = Blueprint("tools", __name__, template_folder="templates")


@tools_bp.route("/healthz")
def healthz():
    try:
        return render_template("healthz.html")
    except TemplateNotFound:
        abort(404)
