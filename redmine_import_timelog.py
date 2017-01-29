#!/usr/bin/env python

import argparse, os, pandas, yaml, datetime, pytz, functools, redmine


def get_config(config_file):
    with open(config_file, 'r') as f:
        return yaml.load(f)


def get_redmine_client(config):
    return redmine.Redmine(config['redmine-base-address'],
                           key = config['api-key'])


def make_datetime(row, timezone):
    return datetime.datetime(year = row['year'],
                             month = row['month'],
                             day = row['day'],
                             hour = row['hour'],
                             minute = row['minute'],
                             tzinfo = timezone)


_TS_COLUMNS = ['year',
               'month',
               'day',
               'hour',
               'minute']
def load_timelog(fpath, timezone):
    log = pandas.read_csv(fpath,
                          encoding = 'utf8')
    log['timestamp'] = log[_TS_COLUMNS] \
                            .fillna(method = 'pad') \
                            .astype('int') \
                            .apply(functools.partial(make_datetime,
                                                     timezone = timezone),
                                   axis = 1)
    log.set_index('timestamp', inplace = True)
    log.drop(_TS_COLUMNS,
             axis = 1,
             inplace = True)
    log['duration_minutes'] = list(pandas.Series(log.index) \
        .groupby([d.date() for d in log.index]) \
        .apply(lambda t: (t.shift(-1).fillna(method = 'pad') - t) \
               .apply(lambda td: td.total_seconds() / 60.0)))
    log['duration_hours'] = log['duration_minutes'] / 60.0
    return log


def create_timeentry(client, ts, timelog_row):
    e = client.time_entry.new()
    e.comments = timelog_row['action']
    e.issue_id = int(timelog_row['redmine task id'])
    e.spent_on = datetime.date(year = ts.year,
                               month = ts.month,
                               day = ts.day)
    e.hours = timelog_row['duration_hours']
    #activity_id: the id of the time activity. This parameter is required unless a default activity is defined in Redmine.
    e.comments = timelog_row['action']
    e.save()


def import_timelog(client, timelog):
    for ts in timelog.index:
        try:
            int(timelog.loc[ts, 'redmine task id'])
        except ValueError:
            continue
        create_timeentry(client, ts, timelog.loc[ts])


DATE_FORMAT = '%Y-%m-%dT%H:%M'


if __name__ == '__main__':
    aparser = argparse.ArgumentParser()
    aparser.add_argument('--config',
                         type = str,
                         default = os.path.join(os.path.dirname(__file__),
                                                'config.yaml'),
                         help = 'Path to config file')
    aparser.add_argument('--since',
                         type = str,
                         default = datetime.date.today().strftime(DATE_FORMAT),
                         help = 'Path to config file')
    aparser.add_argument('filename',
                         type = str,
                         help = 'CSV-file with timelog')

    args = aparser.parse_args()

    conf = get_config(args.config)
    client = get_redmine_client(conf)

    tz = pytz.timezone(conf['time-zone'])

    timelog = load_timelog(args.filename, tz)

    since = datetime.datetime.strptime(args.since, DATE_FORMAT).replace(tzinfo = tz)
    import_timelog(client, timelog[timelog.index >= since])
