from flask import render_template, request, redirect
from datetime import datetime as dt
from task.engine import compute, do_the_rest
from flask import Blueprint
import threading

result_b = Blueprint('result_b', __name__)


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
