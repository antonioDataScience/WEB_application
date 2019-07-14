from flask import request, redirect
from datetime import datetime as dt
from task.view.engine.engine import compute, do_the_rest
from stream_tools.kafka_ import consumer
from task.model.form import MeanForm
from task.model.formR import MeanFormInherit
from task.model.formA import SparkFormInherit
from flask import render_template, Response
from flask import Blueprint
import threading


result_b = Blueprint('result', __name__)
simulation_b = Blueprint('simulation', __name__)
hints_b = Blueprint('hints', __name__)
stream_b = Blueprint('stream', __name__)
rolling_b = Blueprint('rolling', __name__)
average_b = Blueprint('average', __name__)
index_b = Blueprint('index', __name__)


def check_RW(links, result, start, end, stream=False):
    if int(result['rolling_window']) <= (end - start).days:
        return do_the_rest(links, [start, end], int(result['rolling_window']), stream)
    else:
        return {"Error": "Rolling window is wrong!"}


@result_b.route('/result', methods=['POST', 'GET'])
def result():
    if request.method == 'POST':
        result = request.form
        start = dt.strptime(result['date_from'], "%Y-%m-%d")
        end = dt.strptime(result['date_to'], "%Y-%m-%d")
        url = result['url']
        links = compute(url, [start, end])
        if start <= end and links:
            ctx = result['submit']
            if ctx == 'Stream':
                t = threading.Thread(target=do_the_rest, args=(links, [start, end], (end - start).days, True))
                t.start()
                return redirect("/simulation")
            elif ctx == 'Roll':
                result = check_RW(links, result, start, end)
            elif ctx == 'Mean':
                result = do_the_rest(links, [start, end], (end - start).days, False)
            else:
                result = {"Error": "Something is wrong"}
        else:
            return {"Error": "Wrong dates!"}
        return render_template("result.html", result=result)


@simulation_b.route('/simulation')
def kafkaStream():
    consumer.subscribe(topics=['streaming_data_backward'])
    def generate():
        for message in consumer:
            if message is not None:
                yield str(message.value)
                yield " , "
    return Response(generate())


@hints_b.route('/hints', methods=['GET', 'POST'])
def explanation():
    return render_template('hints.html')


@stream_b.route('/stream', methods=['GET', 'POST'])
def stream():
    form = SparkFormInherit()
    return render_template('stream.html', form=form)


@rolling_b.route('/rolling', methods=['GET', 'POST'])
def rolling():
    form = MeanFormInherit()
    return render_template('rolling_mean.html', form=form)


@average_b.route('/average', methods=['GET', 'POST'])
def average():
    form = MeanForm()
    return render_template('average.html', form=form)


@index_b.route('/')
def index():
    return render_template('index.html')
