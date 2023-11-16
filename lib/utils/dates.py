from datetime import datetime, timedelta
import time

# def datetime_str_to_int(datetime_str: str) -> int:
#     datetime_YMD = datetime_str.split(' ')[0]
#     datetime_object = datetime.strptime(datetime_YMD, "%Y-%m-%d")
#     date_int = int(time.mktime((datetime_object).timetuple()))
#     return date_int

# def time_str_to_int(time_str: str) -> int:
#     time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
#     time_int = int(time.mktime((time_object).timetuple()))
#     return time_int

# def datetime_obj_to_int(datetime) -> int:
#     time_int = int(time.mktime((datetime).timetuple()))
#     return time_int

# def timestr_to_datestr(time_str: str) -> str:
#     return time_str.split(' ')[0]

# def timestr_to_minute_int(time_str: str) -> int:
#     time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
#     time_minute = time_object.replace(second=0, microsecond=0)
#     time_int = int(time.mktime((time_minute).timetuple()))
#     return time_int

# def timestr_to_minutestr(time_str: str) -> str:
#     time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
#     time_minute = time_object.replace(second=0, microsecond=0)
#     return str(time_minute)
"""
Legacy Above.
"""


def datetime_to_timestamp(datetime: datetime) -> int:
    return int(datetime.timestamp())


def datetime_to_str(datetime: datetime) -> str:
    return datetime.strftime("%Y-%m-%d %H:%M:%S")


def str_to_timestamp(time_str: str) -> int:
    return datetime_to_timestamp(
        datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S"))


def str_to_datetime(time_str: str) -> datetime:
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")


""" minute """


def timestr_to_minute_obj(time_str: str) -> str:
    date_time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return date_time_obj.replace(second=0, microsecond=0)


def timestr_to_minute_int(time_str: str) -> int:
    return datetime_to_timestamp(timestr_to_minute_obj(time_str))


def timestr_to_minutestr(time_str: str) -> str:
    return datetime_to_str(timestr_to_minute_obj(time_str))


""" day """


def timestr_to_day_obj(time_str: str) -> str:
    date_time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return date_time_obj.replace(hour=0, minute=0, second=0)


def timestr_to_day_int(time_str: str) -> int:
    return datetime_to_timestamp(timestr_to_day_obj(time_str))


def timestr_to_daystr(time_str: str) -> str:
    return datetime_to_str(timestr_to_day_obj(time_str))


def dt_to_dayobj(dt: datetime) -> int:
    return dt.replace(hour=0, minute=0, second=0)


def dt_to_dayint(dt: datetime) -> int:
    daydt = dt_to_dayobj(dt)
    return datetime_to_timestamp(daydt)


""" week """


def timestr_to_week_obj(time_str: str) -> str:
    date_time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return dt_to_weekobj(date_time_obj)


def timestr_to_week_int(time_str: str) -> int:
    return datetime_to_timestamp(timestr_to_week_obj(time_str))


def timestr_to_weekstr(time_str: str) -> str:
    return datetime_to_str(timestr_to_week_obj(time_str))


def dt_to_weekobj(dt: datetime) -> int:
    monday_date_time_obj = dt - timedelta(days=dt.weekday())
    return monday_date_time_obj.replace(hour=0, minute=0, second=0)


def dt_to_weekint(dt: datetime) -> int:
    weekdt = dt_to_weekobj(dt)
    return datetime_to_timestamp(weekdt)


""" month """


def timestr_to_month_obj(time_str: str) -> str:
    date_time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return date_time_obj.replace(day=1, hour=0, minute=0, second=0)


def timestr_to_month_int(time_str: str) -> int:
    return datetime_to_timestamp(timestr_to_month_obj(time_str))


def timestr_to_monthstr(time_str: str) -> str:
    return datetime_to_str(timestr_to_month_obj(time_str))


def dt_to_monthobj(dt: datetime) -> int:
    return dt.replace(day=1, hour=0, minute=0, second=0)


def dt_to_monthint(dt: datetime) -> int:
    monthdt = dt_to_monthobj(dt)
    return datetime_to_timestamp(monthdt)
