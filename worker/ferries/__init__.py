from worker.celery import make_app

(app, global_vars) = make_app(name="ferries", init_fn="worker.ferries.logic:init")
