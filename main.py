from task.form import MeanForm
from task.formR import MeanFormInherit
from task.formA import SparkFormInherit
from flask import Flask, render_template, Response
from flask_navigation import Navigation
from task.services import result_b
from stream_tools.kafka_ import consumer

app = Flask(__name__, template_folder='task/templates')
app.register_blueprint(result_b)
nav = Navigation(app)


@app.route('/simulation')
def kafkaStream():
    consumer.subscribe(topics=['streaming_data_backward'])
    def generate():
        for message in consumer:
            if message is not None:
                yield str(message.value)
                yield " , "
    return Response(generate())


@app.route('/hints', methods=['GET', 'POST'])
def explanation():
    return render_template('hints.html')


@app.route('/stream', methods=['GET', 'POST'])
def stream():
    form = SparkFormInherit()
    return render_template('stream.html', form=form)


@app.route('/rolling', methods=['GET', 'POST'])
def rolling():
    form = MeanFormInherit()
    return render_template('rolling_mean.html', form=form)


@app.route('/average', methods=['GET', 'POST'])
def average():
    form = MeanForm()
    return render_template('average.html', form=form)


@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0', port = 8080)