from datetime import datetime, timedelta

def missing_dates(sdate, edate, existing_dates: list):
    dates = dates_between(sdate, edate)
    new_dates = []
    for date in dates:
        if date not in existing_dates:
            new_dates.append(date)
    return new_dates

def create_datetime(datestring):
    return datetime.strptime(datestring, "%Y-%m-%d")

def dates_between(sdate, edate):
    return [sdate+timedelta(days=x) for x in range((edate-sdate).days)]