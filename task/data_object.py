from datetime import datetime
import re


class Mean():

    def __init__(self, window):
        self.dict = {}
        self.window = window
        self.rolling_mean_results = []
        self.corrections = []
        self.late = []
        self.step = 0

    def handler(self, row_date, row_value):
        if row_date not in self.dict:
            self.dict[row_date] = {'km': row_value, 'N': 1, 'mean': row_value}
        else:
            self.dict[row_date] = {'km': self.dict[row_date]['km'] + row_value,
                                   'N': self.dict[row_date]['N'] + 1,
                                   'mean': self.mean(row_value,
                                                     self.dict[row_date]['N'],
                                                     self.dict[row_date]['mean'])}
        keys = list(self.dict.keys())
        if keys.index(row_date) < self.step:
            self.late.append([keys[self.step:], row_date])
            keys.sort()
            temp_keys = [x for x in keys if keys.index(row_date) - self.window + 1 <= keys.index(x)
                                        and keys.index(row_date) + self.window - 1 >= keys.index(x)]
            self.rolling_mean(temp_keys, 0)
        keys.sort()
        keys = keys[self.step:]
        self.rolling_mean(keys, 1)

    def mean(self, new_sample, N, mean):
        mean -= mean / N
        mean += new_sample / N
        return mean

    def rolling_mean(self, keys, count):
        try:
            self.mean_helper = self.dict[keys[0]]['mean']
        except:
            return
        if self.window > len(keys):
            return
        if self.window < len(keys):
            for key in keys[:self.window]:
                self.mean_helper = self.mean(self.dict[key]['mean'],
                                             len(self.rolling_mean_results) + 1,
                                             self.mean_helper)
            self.step += count
            if count:
                self.rolling_mean_results.append((keys[:self.window], self.mean_helper))
            else:
                self.corrections.append((keys[:self.window], self.mean_helper))
            self.rolling_mean(keys[1:], count)

    def temp_process_data(self, row, start, end, stream=False):
        date = row[1].rsplit(" ", 1)[0]
        match = re.search('\d{4}-\d{2}-\d{2}', date)
        given_date = datetime.strptime(match.group(), '%Y-%m-%d')
        if start <= given_date and given_date <= end:
            if not stream:
                self.handler(date, float(row[4]))
                return True
            else:
                return [date, float(row[4])]
        elif start <= given_date and end <= given_date\
                                 and end.month == given_date.month\
                                 and end.year == given_date.year:
            return False
        else:
            return True