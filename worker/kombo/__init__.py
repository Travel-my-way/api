from worker.celery import make_app

app, global_vars = make_app(name="kombo", init_fn="worker.kombo.logic:init")
