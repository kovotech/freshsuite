import datetime as dt

def fw_date_range_checker(dict_,fieldName,startDate,days=None):
    # startDate = dt.datetime.now()
    if days is not None:
        endDate = startDate-dt.timedelta(days=days)
        record_date_str = dict_[fieldName]
        date1 = str(record_date_str).split('T')
        date3 = dt.datetime.strptime(date1[0],'%Y-%m-%d')
        # print(f"{date3}...................{endDate}",flush=True)
        if date3 > endDate:
            return 'Yes'
        else:
            return 'No'
    else:
        print(f"Input Days")