from task.model.formR import MeanForm
from wtforms import SubmitField


class SparkFormInherit(MeanForm):
    submit = SubmitField("Stream")