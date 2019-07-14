from flask import Flask
from flask_navigation import Navigation
from task.view.views import result_b, simulation_b, hints_b
from task.view.views import stream_b, rolling_b, average_b, index_b


app = Flask(__name__, template_folder='task/templates')
app.register_blueprint(result_b)
app.register_blueprint(simulation_b)
app.register_blueprint(hints_b)
app.register_blueprint(stream_b)
app.register_blueprint(rolling_b)
app.register_blueprint(average_b)
app.register_blueprint(index_b)

nav = Navigation(app)

if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0', port = 8080)