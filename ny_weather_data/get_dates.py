import datetime

def get_dates():
    # Find Range of Dates
    d1 = datetime.date(2019,2,21)
    d2 = datetime.date(2022,1,1)

    dd = [d1 + datetime.timedelta(days=x) for x in range((d2-d1).days + 1)]
    date_list = []

    for d in dd:
        date_list.append(str(d))
    return date_list

print(get_dates())