import os
import sys

sys.path.insert(0, os.path.abspath('..'))

from graffiti_monkey.core import GraffitiMonkey


def lambda_handler(event, context):
    monkey_lambda = GraffitiMonkey(region='us-east-1',
                                   profile=None,
                                   instance_tags_to_propagate=['Name'],
                                   volume_tags_to_propagate=['Name', 'instance_id', 'device'],
                                   volume_tags_to_be_set=[],
                                   snapshot_tags_to_be_set=[],
                                   dryrun=False,
                                   append=False,
                                   volumes_to_tag="",
                                   snapshots_to_tag="",
                                   instance_filter=[],
                                   novolumes=False,
                                   nosnapshots=False)
    monkey_lambda.propagate_tags()

    return "Graffiti Monkey Lambda FTW"
