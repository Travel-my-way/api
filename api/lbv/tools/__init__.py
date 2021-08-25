from flask import Blueprint, render_template, jsonify, current_app
from celery import signature, chord

tools_bp = Blueprint("tools", __name__, template_folder="templates")


@tools_bp.route("/healthz")
def healthz():
    return render_template("healthz.html")


@tools_bp.route("/celery")
def celery_test():
    r = current_app.extensions["celery"].send_tasks(
        from_loc="123456", to_loc="654321", start_date="20210822"
    )
    return jsonify({"uuid": r.id}), 201
