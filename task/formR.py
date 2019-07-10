from task.form import MeanForm
from wtforms import IntegerField, SubmitField


class MeanFormInherit(MeanForm):
    rolling_window = IntegerField(label='Rolling window:', default=5)
    submit = SubmitField("Roll")