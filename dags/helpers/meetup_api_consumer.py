#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import json
import logging
import time
from datetime import datetime, timedelta, date

import requests
from helpers.entity_api_consumer import get_description_entities

api_version = 2
api_key='75525d6b7a4b1d66587f4748147fd7d'


def get_categories():
    resp = requests.get('https://api.meetup.com/{}/categories?key={}'.format(api_version, api_key))
    if resp.status_code != 200:
        # This means something went wrong.
        raise Exception('GET /tasks/ {}'.format(resp.status_code))
    [ logging.info('{} : {}'.format(category['id'], category['name'])) for category in resp.json()['results'] ]


def get_groups(category, country):
    groups = []
    page_size = 200
    offset=0
    total_group_num = 0
    sec_to_sleep=5

    while True :

        url = 'https://api.meetup.com/find/groups' \
              '?host=public&key={key}&category={category}&country={country}&page={page}&offset={offset}'\
            .format(key=api_key, category=category, country=country, page=page_size, offset=offset)

        logging.info(url)

        resp = requests.get(url)
        if resp.status_code != 200:
            if resp.status_code == 429 or resp.status_code == 503:
                logging.info('Status code {} returned '.format(resp.status_code))
                logging.info('Sleeping for {} seconds ...'.format(sec_to_sleep))
                time.sleep(sec_to_sleep)
                sec_to_sleep *= 2
                continue
            elif resp.status_code == 400:
                pass
            else:
                # This means something went wrong.
                raise Exception('GET /tasks/ {}'.format(resp.status_code))
        try:
            for group in resp.json():
                groups.append(group)

                # DEBUG
                logging.info('[{city}] {id} - {name} '.format(id=group['id'], name=group['name'], city=group['city']))

        except ValueError:
            pass

        total_group_num += len(resp.json())
        offset += 1
        sec_to_sleep = 5

        if len(resp.json()) < page_size:
            break

    # DEBUG
    logging.info('Total groups number : {}'.format(total_group_num))
    return groups


def get_events(group_id, starting_from, to, status):
    events = []
    page_size = 200
    offset = 0
    total_num = 0
    sec_to_sleep = 5

    logging.info(' Getting event for Group: {group_id} from : {starting_time} -> {to}'
                 .format(group_id=group_id,
                         starting_time=time.strftime('%Y-%m-%d', time.localtime(starting_from)),
                         to=time.strftime('%Y-%m-%d', time.localtime(to))))

    while True:

        url = 'https://api.meetup.com/2/events' \
              '?host=public&key={key}&group_id={group_id}&page={page}&offset={offset}&time={fromt},{tot}&status={status}' \
            .format(key=api_key, group_id=group_id, page=page_size, offset=offset, fromt=starting_from * 1000, tot=to * 1000, status=status)
        logging.info(url)
        resp = requests.get(url)

        if resp.status_code != 200:
            if resp.status_code == 429 or resp.status_code == 503:
                logging.info('Status code {} returned '.format(resp.status_code))
                logging.info('Sleeping for {} seconds ...'.format(sec_to_sleep))
                time.sleep(sec_to_sleep)
                sec_to_sleep*=2
                continue
            elif resp.status_code == 400:
                pass
            else:
                # This means something went wrong.
                raise Exception('GET /tasks/ {}'.format(resp.status_code))
        try:
            for event in resp.json()['results']:
                events.append(event)

                # DEBUG
                ev_time = datetime.utcfromtimestamp(event['time'] / 1000 ).strftime('%Y-%m-%d %H:%M:%S')
                logging.info('[{time}] {id} - {name} '.format(id=event['id'], name=event['name'], time=ev_time))


            total_num += int(resp.json()['meta']['total_count'])
            offset += 1
            sec_to_sleep=5

            if int(resp.json()['meta']['total_count']) < page_size:
                break
        except ValueError:
            pass
        except KeyError:
            pass
        # DEBUG
        logging.info('Total events number : {}'.format(total_num))
    return events


def encapsulate_event_model(event):
    new_event = copy.deepcopy(event)
    try:
        del new_event['utc_offset']
        del new_event['rsvp_limit']
        del new_event['waitlist_count']
        del new_event['created']
        del new_event['maybe_rsvp_count']
        del new_event['photo_url']
        del new_event['updated']
        key_words = get_description_entities(new_event['description'])
        new_event['key_word'] = key_words
    except KeyError as e:
        logging.info('key error exception : {}'.format(e))
    return new_event

def extractDailyData(ds, **kwargs):
    export_dir = kwargs['params']['dir']
    events_count = 0

    logging.info('Exporting to {} directory'.format(export_dir))

    output_groups_file=open('{dir}/output_groups_{date}.txt'.format(dir=export_dir, date=ds), "w+")
    output_events_file=open('{dir}/output_events_{date}.txt'.format(dir=export_dir, date=ds), "w+")

    starting_from = int(time.mktime((datetime.strptime(ds, '%Y-%m-%d').date() - timedelta(1)).timetuple()))
    to = int(time.mktime(time.strptime(ds, '%Y-%m-%d')))

    groups = get_groups(34, 'fr')
    for group in groups:
        output_groups_file.write(str(json.dumps(group))+'\n')
    for i in range(0, len(groups), 200):
        group_list = ','.join(str(v) for v in list(map(lambda g: g['id'], groups[i:i+200])))
        for ev in get_events(group_list, starting_from, to, 'past'): # Here we should modify the date
            event = encapsulate_event_model(ev)
            output_events_file.write(str(json.dumps(event))+'\n')
            events_count += 1
    output_groups_file.close()
    output_events_file.close()

    logging.info('Total retrieved groups : {}'.format(len(groups)))
    logging.info('Total retrieved events : {}'.format(events_count))
