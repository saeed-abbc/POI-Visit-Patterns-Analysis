import pandas as pd
import os

from datetime import date
from dateutil.rrule import rrule, WEEKLY
import matplotlib.pyplot as plt

base_path = os.path.join(os.getcwd(), 'ProcessedWeeklyPatterns')

def get_visits():
    start = date(2020, 5, 4)
    end = date(2021, 5, 10)

    week_list, visit_list = [], []

    for dt in rrule(WEEKLY, dtstart=start, until=end):

        week = dt.strftime("%Y-%m-%d")
        week_list.append(week)

        file_path = os.path.join(base_path, week+'-weekly-patterns.csv')
        df = pd.read_csv (file_path)
        visit_counts = df['raw_visit_counts'].sum()
        visit_list.append(visit_counts)

    return week_list, visit_list


def preprocess_dailyCases(week_list):

    file_path = os.path.join(os.getcwd(), 'daily_cases', 'covid19-download.csv')
    df = pd.read_csv (file_path)
    df = df[df['prname']=='Ontario'] 
    df = df[df['date'].isin(week_list)] 
    df = df[['date', 'numconf']]
    df.to_csv('weekly_cases.csv')


def get_weeklyCases():

    case_list = {}

    file_path = os.path.join(os.getcwd(), 'daily_cases', 'weekly_cases.csv')
    df = pd.read_csv (file_path)
    case_list = list(df.weekly_num.values)

    return case_list

def plot(visit_list, case_list):
    plt.title('Visits vs Cases')
    plt.plot(visit_list, label = 'visits')
    plt.plot(case_list, label = 'cases')
    plt.ylabel('counts')
    months = ['May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr']
    plt.xticks(np.linspace(0,54,12), months)
    plt.legend()
    plt.savefig('viz/correlation')
    plt.show()


if __name__ == '__main__':
    week_list, visit_list = get_visits()
    case_list = get_weeklyCases()
    plot(visit_list, case_list)
    
