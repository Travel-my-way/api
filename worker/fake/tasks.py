from . import app
from worker import wrappers
from worker.exception import WorkerException


@app.task(name="worker", bind=True)
@wrappers.catch()
def worker(self, from_loc, to_loc, start_date, nb_passenger):
    raise WorkerException("I'm a fake worker dude !")
