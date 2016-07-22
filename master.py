from datetime import datetime, timedelta

from ptocore.analyzercontext import AnalyzerContext
from ptocore.sensitivity import margin
from ptocore.collutils import grouper

import json

ac = AnalyzerContext()

OFFSET = timedelta(hours=12)

max_action_id, timespans = margin(OFFSET, ac.action_set)

# only one timespan at a time
time_from, time_to = timespans[0]

ac.set_result_info(max_action_id, [(time_from, time_to)])

# get all input observations within timespan
input_types = [
    'ecn.connectivity.works',
    'ecn.connectivity.broken',
    'ecn.connectivity.transient',
    'ecn.connectivity.offline'
]

stages = [
    {
        '$match': {
            'action_ids.0.valid': True,
            'conditions': {'$in': input_types},
            'time.from': {'$gte': time_from},
            'time.to': {'$lte': time_to}
        }
    },
    {
        '$unwind': '$conditions'
    },
    {
        '$match': {
            'conditions': {'$in': input_types}
        }
    },
    {
        '$group': {
            '_id': {'$arrayElemAt': ['$path', -1]},
            'sips': {'$addToSet': {'$arrayElemAt': ['$path', 0]}},
            'conditions': {'$addToSet': '$conditions'},
            'obs': {'$push': '$_id'},
            'time_from': {'$min': '$time.from'},
            'time_to': {'$max': '$time.to'}
        }
    },
    {
        '$project': {
            'conditions': 1,
            'num_sips': {'$size': '$sips'},
            'sips': 1,
            'obs': 1,
            'time_from': 1,
            'time_to': 1
        }
    },
    {
        '$match': {
            'num_sips': {'$gte': 3}
        }
    },
    {
        '$project': {
            'any_broken': {'$anyElementTrue': {
                    '$map': {
                        'input': '$conditions',
                        'as': 'condition',
                        'in': {'$eq': ['$$condition', 'ecn.connectivity.broken']}
                    }
                }
            },
            'any_works': {'$anyElementTrue': {
                    '$map': {
                        'input': '$conditions',
                        'as': 'condition',
                        'in': {'$eq': ['$$condition', 'ecn.connectivity.works']}
                    }
                }
            },
            'path': ['*', '$_id'],
            'value': {'sips': '$sips'},
            'sources.obs': '$obs',
            'time.from': '$time_from',
            'time.to': '$time_to'
        }
    },
    {
        '$match': {
            'any_broken': True
        }
    },
    {
        '$project': {
            'conditions': {'$cond':
                {
                    'if': '$any_works',
                    'then': ['ecn.path_dependent'],
                    'else': ['ecn.site_dependent']
                }
            },
            'path': 1,
            'value': 1,
            'sources.obs': 1,
            'time.from': 1,
            'time.to': 1,
            '_id': 0
        }
    }
]

cursor = ac.observations_coll.aggregate(stages, allowDiskUse=True)
for obs in grouper(cursor, 1000):
    ac.temporary_coll.insert_many(list(obs))


