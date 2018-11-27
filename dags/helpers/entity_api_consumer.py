#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Imports the Google Cloud client library
import json
import os
import logging

from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
from google.protobuf.json_format import MessageToJson

# Get actual path
resources_path = '{}/resources'.format(os.environ.get('DAGS_FOLDER'))

def get_description_entities(description):
    client = language.LanguageServiceClient.from_service_account_json(
        '{}/KEY_FILE_JSON.json'.format(resources_path))

    # The text to analyze
    text = u'{}'.format(description)
    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)

    # Detects the sentiment of the text
    entities = client.analyze_entities(document=document).entities

    # entity types from enums.Entity.Type
    entity_type = ('UNKNOWN', 'PERSON', 'LOCATION', 'ORGANIZATION',
                   'EVENT', 'WORK_OF_ART', 'CONSUMER_GOOD', 'OTHER')
    entities_dict=[]
    for entity in entities:
        if entity.salience > 0.01:
            logging.info('=' * 20)
            logging.info(u'{:<16}: {}'.format('name', entity.name))
            logging.info(u'{:<16}: {}'.format('type', entity_type[entity.type]))
            logging.info(u'{:<16}: {}'.format('metadata', entity.metadata))
            logging.info(u'{:<16}: {}'.format('salience', entity.salience))
            logging.info(u'{:<16}: {}'.format('wikipedia_url',
                                       entity.metadata.get('wikipedia_url', '-')))
            entities_dict.append(json.loads(str(MessageToJson(entity))))
    return entities_dict

