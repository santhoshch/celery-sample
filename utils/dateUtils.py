###############################################################################
#       Copyright Gnana, Inc 2012
#       All rights reserved
#       This code may not be used without written permission from Gnana, Inc.
#       Hessam Tehrani July 2012
###############################################################################

from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import pytz
import re
import logging
from celery_sample import sec_context
#from domainmodel.uip import ALL_PERIODS_CACHE

logger = logging.getLogger('gnana.%s' % __name__)

excelBase = datetime(1899, 12, 30, 0, 0, 0)
excelBaseDate = date(1899, 12, 30)
base_epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
onems = timedelta(milliseconds=1)


# lambdas for convenience
datetime2xl = lambda dt: EpochClass.from_datetime(dt).as_xldate()
datetime2epoch = lambda dt: EpochClass.from_datetime(dt).as_epoch()
datetime2pytime = lambda dt: EpochClass.from_datetime(dt).as_pytime()

xl2datetime = lambda xl_dt: EpochClass.from_xldate(xl_dt).as_datetime()
xl2epoch = lambda xl_dt: EpochClass.from_xldate(xl_dt).as_epoch()
xl2pytime = lambda xl_dt: EpochClass.from_xldate(xl_dt).as_pytime()

# if you excel date is in tenant time zone, you should use below conversions
xl2datetime_ttz = lambda xl_dt: EpochClass.from_xldate(xl_dt, False).as_datetime()
xl2epoch_ttz = lambda xl_dt: EpochClass.from_xldate(xl_dt, False).as_epoch()
xl2pytime_ttz = lambda xl_dt: EpochClass.from_xldate(xl_dt, False).as_pytime()

epoch2datetime = lambda ep: EpochClass.from_epoch(ep).as_datetime()
epoch2pytime = lambda ep: EpochClass.from_epoch(ep).as_pytime()
epoch2xl = lambda ep: EpochClass.from_epoch(ep).as_xldate()

pytime2datetime = lambda ep: EpochClass.from_pytime(ep).as_datetime()
pytime2epoch = lambda ep: EpochClass.from_pytime(ep).as_epoch()
pytime2xl = lambda ep: EpochClass.from_pytime(ep).as_xldate()

datestr2xldate = lambda closedDate: EpochClass.from_string(cleanmmddyy(closedDate),
                                                           '%m/%d/%Y',
                                                           timezone='tenant').as_xldate()

fuzzydateinput2xldate = lambda date: fuzzydateinput(date).as_xldate()

ALL_PERIODS_CACHE = {}

class EpochClass(object):
    """ epoch function returns an object of this type which can be used as long for all
    computations. However printing the values will result in displaying the value
    as a date time in the timezone preference based on the shell.
    """

    def __init__(self, ts=None):
        self.timezone = get_tenant_timezone()
        if ts is None:
            import time
            ts = time.time()
        self.ts = ts

    def __str__(self):
        return "%s" % datetime.fromtimestamp(self.ts, self.timezone)

    __repr__ = __str__

    def as_datetime(self):
        logger.info('ts %s - zone %s',self.ts, self.timezone)
        return datetime.fromtimestamp(self.ts, self.timezone)

    def as_xldate(self):
        d = self.as_datetime()
        d = d.astimezone(pytz.utc)
        e = datetime(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond)
        delta = e - excelBase
        return delta.days + (float(delta.seconds) / (3600 * 24))

    def as_tenant_xl_int(self):
        '''
        Useful for passing to the holidays which are defined as tenant excel date integers
        '''
        d = self.as_datetime()
        e = datetime(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond)
        delta = e - excelBase
        return delta.days

    def as_pytime(self):
        return self.ts

    def as_epoch(self):
        return long(self.ts * 1000)

    def __eq__(self, other):
        if isinstance(other, EpochClass):
            return self.ts == other.ts
        elif isinstance(other, (int, long)):
            return self.as_epoch() == other
        elif isinstance(other, datetime):
            if not other.tzinfo:
                raise Exception("Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() == other
        else:
            return False

    def __ne__(self, other):
        return not (self.__eq__(other))

    def __lt__(self, other):
        if isinstance(other, EpochClass):
            return self.ts < other.ts
        elif isinstance(other, (int,long)):
            return self.as_epoch() < other
        elif isinstance(other, datetime):
            if not other.tzinfo:
                raise Exception("Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() < other
        else:
            return False

    def __le__(self, other):
        if isinstance(other, EpochClass):
            return self.ts <= other.ts
        elif isinstance(other, (int, long)):
            return self.as_epoch() <= other
        elif isinstance(other, datetime):
            if not other.tzinfo:
                raise Exception("Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() <= other
        else:
            return False

    def __gt__(self, other):
        if isinstance(other, EpochClass):
            return self.ts > other.ts
        elif isinstance(other, (int, long)):
            return self.as_epoch() > other
        elif isinstance(other, datetime):
            if not other.tzinfo:
                raise Exception("Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() > other
        else:
            return False

    def __ge__(self, other):
        if isinstance(other, EpochClass):
            return self.ts >= other.ts
        elif isinstance(other, (int, long)):
            return self.as_epoch() >= other
        elif isinstance(other, datetime):
            if not other.tzinfo:
                raise Exception("Comparison to datetime without tzinfo not allowed")
            else:
                return self.as_datetime() >= other
        else:
            return False

    def __add__(self, other):
        if isinstance(other, (int, long)):
            return EpochClass(self.ts + other/1000.0)
        elif isinstance(other, timedelta):
            return EpochClass(self.ts + other.total_seconds())
        elif isinstance(other, relativedelta):
            as_dt = self.as_datetime()
            new_datetime = as_dt + other
            tdelta = new_datetime - as_dt
            whole_days = tdelta.days * 86400
            seconds = tdelta.seconds
            microseconds = tdelta.microseconds
            result_1 = EpochClass(self.ts + (whole_days))
            # check to see if we have to add or subtract hours due to daylight saving
            hour_diff = (result_1.as_datetime().hour - as_dt.hour) % 24
            if hour_diff > 12:
                hour_diff = hour_diff - 24
            return EpochClass(self.ts + (whole_days+seconds+microseconds/1000000.0 - hour_diff * 3600))
        else:
            raise Exception("Unspported type %s.  EpochClass supports integers, timedelta and relativedelta" % type(other).__name__)

    def __sub__(self, other):
        if isinstance(other, (int, long)):
            return EpochClass(self.ts-other/1000.0)
        elif isinstance(other, timedelta):
            return EpochClass(self.ts-other.total_seconds())
        elif isinstance(other, EpochClass):
            return timedelta(seconds=self.ts - other.ts)
        elif isinstance(other, relativedelta):
            as_dt = self.as_datetime()
            new_datetime = as_dt - other
            tdelta = new_datetime - as_dt
            whole_days = tdelta.days * 86400
            seconds = tdelta.seconds
            microseconds = tdelta.microseconds
            result_1 = EpochClass(self.ts + (whole_days))
            # check to see if we have to add or subtract hours due to daylight saving
            hour_diff = (result_1.as_datetime().hour - as_dt.hour) % 24
            if hour_diff > 12:
                hour_diff = hour_diff - 24
            return EpochClass(self.ts + (whole_days + seconds + microseconds / 1000000.0 - hour_diff * 3600))
        else:
            raise Exception("Unspported type %s.  EpochClass supports integers, EpochClass and timedelta" % type(other).__name__)

    def __hash__(self):
        return hash(self.ts)

    @classmethod
    def from_pytime(cls, f):
        return cls(f)

    @classmethod
    def from_datetime(cls, dt):
        if not dt.tzinfo:
            raise Exception("Using naive datetime is very confusing with epoch")
        else:
            utc_time = dt.astimezone(pytz.utc)
            delta = utc_time - base_epoch
            return cls(delta.total_seconds())

    @classmethod
    def from_xldate(cls, xlfloat, utc=True):
        # days = int(xlfloat)
        # secondsF = (xlfloat - days) * 3600 * 24
        # seconds = int(secondsF)
        # microseconds = int(secondsF * 1000 - seconds * 1000)
        # naive_dt = excelBase + timedelta(days, seconds, microseconds)
        naive_dt = excelBase + timedelta(days=int(xlfloat), seconds=(xlfloat - int(xlfloat)) * 3600 * 24)
        if utc:
            dt = pytz.utc.localize(naive_dt)
        else:
            tz_info = get_tenant_timezone()
            dt = tz_info.localize(naive_dt, tz_info.dst(naive_dt))
        return cls.from_datetime(dt)

    @classmethod
    def from_epoch(cls, ep):
        return cls(float(ep) / 1000)

    @classmethod
    def from_string(cls, s, fmt, timezone='utc'):
        if timezone == 'utc':
            tzinfo = pytz.utc
        elif timezone == 'tenant':
            tzinfo = get_tenant_timezone()
        else:
            tzinfo = pytz.timezone(timezone)
        fuzzy_dt = datetime.strptime(s, fmt)
        return cls.from_datetime(tzinfo.localize(fuzzy_dt, is_dst=tzinfo.dst(fuzzy_dt)))


def epoch(desc="NOW", month=None, day=None, hour=0, mins=0, second=0, utc=True):

    if desc is None:
        raise Exception("Ambiguous value to epoch")
    elif isinstance(desc, basestring):
        if desc == "NOW":
            return EpochClass()
        elif re.match('\d{14}', desc):
            return EpochClass.from_datetime(
                get_tenant_timezone().localize(datetime.strptime(str(desc), '%Y%m%d%H%M%S')))
        else:
            raise Exception('ERROR: epoch() accepts only NOW or tenant date in format YYYYMMDDhhmmss ')
    elif isinstance(desc, datetime):
        return EpochClass.from_datetime(desc)
    elif isinstance(desc, (long, int)) and month is None:
        if desc < 100000:
            return EpochClass.from_xldate(desc, utc=utc)
        else:
            return EpochClass.from_epoch(desc)
    elif isinstance(desc, float):
        if desc < 100000:
            return EpochClass.from_xldate(desc, utc=utc)
        else:
            return EpochClass.from_pytime(desc)
    else:
        return EpochClass.from_datetime(
            get_tenant_timezone().localize(datetime(desc, month, day, hour, mins, second)))


filename_formats = [
    '(\d\d\d\d)(\d\d)(\d\d).(\d\d)(\d\d).csv',
    '(\d\d)(\d\d)(\d\d).(\d\d)(\d\d).csv',
    '(\d\d\d\d)(\d\d)(\d\d).csv',
    '(\d\d)(\d\d)(\d\d).csv',
]


def getDatetime(filename):
    for f in filename_formats:
        match = re.search(f, filename)
        if(match):
            break
    if(match):
        match_count = len(match.groups())
        year = int(match.group(1))
        if(year < 100):
            year += year < 67 and 2000 or 1900
        return datetime(year, int(match.group(2)), int(match.group(3)),
                        match_count > 3 and int(match.group(4)) or 0,
                        match_count > 4 and int(match.group(5)) or 0,
                        tzinfo=pytz.utc)
    else:
        return None


def get_date_time_for_extended_month(year, month, tzinfo):
    if month < 0:
        year = year - 1
        month += 13
    elif month > 12:
        year += 1
        month -= 12
    dt = datetime(year, month, 1)
    dt = tzinfo.localize(dt, is_dst=tzinfo.dst(dt))
    return dt


import collections
period = collections.namedtuple("period", ["mnemonic", "begin", "end"])


def periods_for_year(y, quarters, period_type, tzinfo):
    if period_type[0] in ('Y', 'y'):
        period_start = get_date_time_for_extended_month(y, quarters[0], tzinfo)
        period_end = get_date_time_for_extended_month(y, quarters[-1], tzinfo) - onems
        return [period(str(y), period_start, period_end,)]
    elif period_type[0] in ('Q', 'q'):
        periods = []
        for i, start_month in enumerate(quarters[:-1]):
            end_month = quarters[i+1]
            periods.append(period("%04dQ%d" % (int(y), int(i+1)),
                           get_date_time_for_extended_month(y, start_month, tzinfo),
                           get_date_time_for_extended_month(y, end_month, tzinfo) - onems,))
        return periods
    elif period_type[0] in ('M', 'm'):
        periods = []
        start_month = quarters[0]
        end_month = quarters[-1]
        # there is no month 0:
        month_range = filter(lambda x: x != 0, range(start_month, end_month))
        for i, month in enumerate(month_range):
            # be careful as there is no month 0
            periods.append(period("%04dM%02d" % (y, i+1),
                           get_date_time_for_extended_month(y, month, tzinfo),
                           get_date_time_for_extended_month(y, month+1 if month+1 else 1, tzinfo) - onems,))
        return periods
    else:
        raise Exception('Unkwon period type: '+period_type)
        return []


def get_all_periods(period_type, filt_out_qs=None):
    tenant_name = sec_context.name
    filtered_qs = ';'.join(sorted(filt_out_qs)) if filt_out_qs is not None else None
    try:
        return ALL_PERIODS_CACHE[tenant_name, period_type, filtered_qs] 
    except KeyError:
        ret_val = _get_all_periods(period_type, filt_out_qs)
        ALL_PERIODS_CACHE[tenant_name, period_type, filtered_qs] = ret_val
        return ret_val


def _get_all_periods(period_type, filt_out_qs=None):
    # Read the configuration
    t = sec_context.details
    period_config = t.get_config('forecast', 'quarter_begins')
    if not period_config:
        raise Exception('No configuration found for the quarter definition. (quarter_begins)')
    tzinfo = get_tenant_timezone()

    if isinstance(period_config, list):
        period_config = {'1990-': period_config}

    # Create a list of all periods
    all_periods = []
    for x in period_config:
        # Find the proper range to use
        if x.endswith('-'):
            start, end = x[0:-1], 2050
        elif x.find('-') == -1:
            start, end = x, x
        else:
            start, end = x.split('-')

        # Convert the range to integers
        start, end = int(start), int(end)
        for y in range(start, end+1):
            all_periods.extend(periods_for_year(y, period_config[x], period_type, tzinfo))
    all_periods = sorted(all_periods)
    # Validate that there are no duplicates
    if len(all_periods) > len(set(map(lambda x1: x1[0], all_periods))):
        raise Exception('Duplicate financial years found')

    if filt_out_qs:
        return [[qmn, beg, end] for qmn, beg, end in all_periods
                if qmn not in filt_out_qs]
    else:
        return all_periods


def get_all_periodsF(period_type, filt_out_qs=None):
    return [[datetime2xl(beg), datetime2xl(end)]
            for qmn, beg, end in get_all_periods(period_type, filt_out_qs)]


def period_details(a_datetime=None, period_type='Q', delta=0, count=1):
    if a_datetime is None:
        a_datetime = datetime.now(tz=pytz.utc)
    all_periods = get_all_periods(period_type)
    for idx, time_span in enumerate(all_periods):
        if time_span[1] <= a_datetime <= time_span[2]:
            if count == 1 or count == 0:
                return all_periods[idx+delta]
            elif delta > 0:
                return list(all_periods[idx+it+delta] for it in range(count))
            else:
                return list(all_periods[idx-it+delta] for it in range(count))
    else:
        raise Exception("Given time doesn't fall into any known time span")


def period_details2(a_datetime=None, period_type='Q', delta=0, count=1):
    delta = int(delta)
    count = int(count)
    assert count, "count cannot be zero for period_details2"
    if a_datetime is None:
        a_datetime = datetime.now(tz=pytz.utc)
    all_periods = get_all_periods(period_type)
    for idx, time_span in enumerate(all_periods):
        if time_span[1] <= a_datetime <= time_span[2]:
            if count > 0:
                return all_periods[(idx+delta):(idx+delta+count)]
            else:
                return all_periods[idx+delta+count+1:idx+delta+1]
    else:
        raise Exception("Given time doesn't fall into any known time span")


def current_period(a_datetime=None, period_type='Q'):
    return period_details(a_datetime, period_type, 0)


def current_periodF(a_xldate=None, period_type='Q'):
    cp = period_details(xl2datetime(a_xldate), period_type, 0)
    return [datetime2xl(cp[1]), datetime2xl(cp[2])]


def next_period(a_datetime=None, period_type='Q', skip=1, count=1):
    return period_details(a_datetime, period_type, delta=skip, count=count)


def prev_period(a_datetime=None, period_type='Q', skip=1, count=1):
    return period_details(a_datetime, period_type, delta=-skip, count=count)


def get_a_date_time_as_float_some_how(date_like_thing):
    if not date_like_thing:
        return None
    # Convert modified date from string to an excel float
    try:
        a_date = float(date_like_thing)
        if (a_date > 100000):
            a_date = EpochClass.from_epoch(a_date).as_xldate()
    except TypeError:
        # TODO: Move the type error to the exception list below so that
        # we will try the fall back mechanism after we see why it is failing first
        logger.error("Unable to determine the date from %s" + str(date_like_thing))
        logger.error('Type of the argument is %s', str(type(date_like_thing)))
        return None
    except (ValueError):
        try:
            a_date = datetime2xl(EpochClass.from_string(cleanmmddyy(a_date), '%m/%d/%Y'))
        except:
            logger.error("Unable to determine the date from " + date_like_thing)
            return None

    return a_date


def date2str(date):
    return str(date.month) + '/' + str(date.day) + '/' + str(date.year)


def cleanmmddyy(anstr):
    a = anstr.split('/')
    if len(a) != 3:
        return anstr
    year = int(a[2])
    if year < 70:
        year += 2000
    return str(int(a[0])) + '/' + str(int(a[1])) + '/' + str(year)


now = lambda: EpochClass().as_datetime()


def getperiods(begin_date, end_date):
    ret = []
    while(end_date > begin_date):
        ret.append(end_date)
        end_date = end_date + timedelta(-7)
    return reversed(ret)

formats = ["%Y/%m/%d", "%y/%m/%d",
           "%Y-%m/%d", "%y-%m-%d",
           "%Y-%b-%d", "%y-%b-%d",
           "%Y/%b/%d", "%y/%b/%d",
           "%m/%d/%Y"]


def fuzzydateinput(olddate, timezone=None):
    """ Converts datetime in one of the accepted string format to datetime
        object. If a timezone name is provided, the datetime is converted to UTC
        and returned.
    """
    if not timezone:
        timezone = 'tenant'

    for x in formats:
        try:
            return EpochClass.from_string(olddate, x, timezone=timezone)
        except ValueError:
            pass
    raise Exception("No formats matched - %s" % olddate)


def convert_date(d):
    if (isinstance(d, datetime)):
        return datetime2xl(d)
    elif (isinstance(d, float)):
        return d
    elif (isinstance(d, int) or isinstance(d, long)):
        return datetime2xl(epoch2datetime(d))
    else:
        raise ValueError()


class TimeHorizon(object):
    """ Manages time horizon (being, as_of, and end) for running
        forecasts.
        The begins, as_of, and horizon (end) take default values as
        the current epoch(), but that's less useful and is meant
        to simplify instantiating this class.
    """
    def __init__(self, begins=None, as_of=None, horizon=None):

        if as_of is None:
            as_of = now()
        self.as_of = as_of

        if begins is None or horizon is None:

            period_details = current_period(self.as_of)

            if begins is None:
                begins = period_details[1]

            if horizon is None:
                horizon = period_details[2]

        self.begins = begins
        self.as_of = as_of
        self.horizon = horizon

    def set_begins(self, d):
        self._begins = convert_date(d)

    def set_as_of(self, d):
        self._as_of = convert_date(d)

    def set_horizon(self, d):
        self._horizon = convert_date(d)

    def get_begins(self):
        return xl2datetime(self._begins)

    def get_beginsF(self):
        return self._begins

    def get_as_of(self):
        return xl2datetime(self._as_of)

    def get_as_ofF(self):
        return self._as_of

    def get_horizon(self):
        return xl2datetime(self._horizon)

    def get_horizonF(self):
        return self._horizon

    begins = property(get_begins, set_begins)
    as_of = property(get_as_of, set_as_of)
    horizon = property(get_horizon, set_horizon)

    as_ofF = property(get_as_ofF)
    beginsF = property(get_beginsF)
    horizonF = property(get_horizonF)

    @classmethod
    def from_dict(cls, time_horizon_dict):
        return cls(time_horizon_dict['_begins'],
                   time_horizon_dict['_as_of'],
                   time_horizon_dict['_horizon'])

    def encode_time_horizon(self):
        """ Returns begins, as_of and horizon epochs separated by a _
        (underscore). This method is used to timestamp the forecast
        results, so multiple runs can be cached.
        """
        return str(datetime2epoch(self.get_begins())) + '_' + \
                   str(datetime2epoch(self.get_as_of())) + '_' + \
                   str(datetime2epoch(self.get_horizon()))


def get_tenant_timezone():

    if sec_context.name and sec_context.name != 'N/A':
        return pytz.timezone("US/Pacific")
    else:
        return pytz.timezone("US/Pacific")


def get_tenant_timezone_offset():
    '''
    Returns timezone offset in minutes based on tenant's timezone. For example,
    if the tanant timezone is PST, it returns -480 (8*60) or -420 based on DST. The number can be
    positive or negative based on the timezone.
    '''
    now = datetime.now()
    localoffset = get_tenant_timezone().localize(now) - pytz.utc.localize(now)
    return -(localoffset.days * 86400 + localoffset.seconds) / 60


def from_db(date_object):
    if date_object.tzinfo:
        return date_object
    else:
        return pytz.utc.localize(date_object)


def set_time_attrs(begins, as_of, horizon):
    if as_of.tzinfo:
        return current_period(as_of)[0]
    else:
        return current_period(pytz.utc.localize(as_of))[0]


class POIDetail(object):
    __slots__ = [
        "__as_of", "__begin", "__horizon",
        "__as_of_epoch", "__begin_epoch", "__horizon_epoch",
        "__as_of_datetime", "__begin_datetime", "__horizon_datetime",
    ]
    __format_str = "%Y%m%d_%H%M%S"

    def __init__(self, as_of, begin=None, horizon=None):
        self.as_of = as_of
        if begin is None or horizon is None:
            cp = current_period(self.as_of)
            if begin is None:
                self.begin = cp["begin"]
            if horizon is None:
                self.horizon = cp["end"]

    @property
    def as_of(self):
        return self.__as_of

    @as_of.setter
    def as_of(self, val):
        print "setting as_of"
        if isinstance(val, EpochClass):
            self.__as_of = val
        else:
            self.__as_of = epoch(val)
        self.__as_of_datetime = self.as_of.as_datetime()
        self.__as_of_epoch = self.as_of.as_epoch()

    @property
    def as_of_epoch(self):
        return self.__as_of_epoch

    @property
    def as_of_datetime(self):
        return self.__as_of_datetime

    @property
    def begin(self):
        return self.__begin

    @begin.setter
    def begin(self, val):
        if isinstance(val, EpochClass):
            self.__begin = val
        else:
            self.__begin = epoch(val)
        self.__begin_datetime = self.as_of.as_datetime()
        self.__begin_epoch = self.as_of.as_epoch()

    @property
    def begin_epoch(self):
        return self.__begin_epoch

    @property
    def begin_datetime(self):
        return self.__begin_datetime

    @property
    def horizon(self):
        return self.__horizon

    @horizon.setter
    def horizon(self, val):
        print "setting horizon"
        if isinstance(val, EpochClass):
            self.__horizon = val
        else:
            self.__horizon = epoch(val)
        self.__horizon_datetime = self.as_of.as_datetime()
        self.__horizon_epoch = self.as_of.as_epoch()

    @property
    def horizon_epoch(self):
        return self.__horizon_epoch

    @property
    def horizon_datetime(self):
        return self.__horizon_datetime

    def __str__(self):
        return "as_of=%s|begin=%s|horizon:%s" % (
            self.as_of_datetime.strftime(POIDetail.__format_str),
            self.begin_datetime.strftime(POIDetail.__format_str),
            self.horizon_datetime.strftime(POIDetail.__format_str),
        )

    __repr__ = __str__
