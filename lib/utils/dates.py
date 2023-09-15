from datetime import datetime
import time

def datetime_str_to_int(datetime_str: str) -> int:
    datetime_YMD = datetime_str.split(' ')[0]
    datetime_object = datetime.strptime(datetime_YMD, "%Y-%m-%d")
    date_int = int(time.mktime((datetime_object).timetuple()))
    return date_int


def time_str_to_int(time_str: str) -> int:
    time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_int = int(time.mktime((time_object).timetuple()))
    return time_int


def datetime_obj_to_int(datetime) -> int:
    time_int = int(time.mktime((datetime).timetuple()))
    return time_int


def timestr_to_datestr(time_str: str) -> str:
    return time_str.split(' ')[0]


def timestr_to_minute_int(time_str: str) -> int:
    time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_minute = time_object.replace(second=0, microsecond=0)
    time_int = int(time.mktime((time_minute).timetuple()))
    return time_int


def timestr_to_minutestr(time_str: str) -> str:
    time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_minute = time_object.replace(second=0, microsecond=0)
    return str(time_minute)
