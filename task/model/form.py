from wtforms import Form
from wtforms import SubmitField, StringField, HiddenField
from wtforms.fields.html5 import DateField



class MeanForm(Form):
    date_from = DateField("Date from:", format ="%Y-%m-%d")
    date_to = DateField("Date to:", format="%Y-%m-%d")
    url = StringField(label='Url:', default="https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
    submit = SubmitField("Mean")



