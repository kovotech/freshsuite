import datetime as dt

def timer(days):
    end_date = dt.datetime.today()
    start_date = end_date-dt.timedelta(days=days)
    end_datetime = end_date.strftime("%Y-%m-%d")
    start_datetime = start_date.strftime("%Y-%m-%d")
    timer = {
        "start" : f"{start_datetime} 00:00:00",
        "end" : f"{end_datetime} 23:59:59"
    }
    return timer