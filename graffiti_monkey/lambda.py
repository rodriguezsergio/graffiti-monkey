import logging

from core import GraffitiMonkey

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)
    kwargs = {
        'region': 'us-east-1',
        'profile': None,
        'instance_tags_to_propagate': ['Name'],
        'volume_tags_to_propagate': ['Name', 'instance_id', 'device'],
        'volume_tags_to_be_set': [],
        'snapshot_tags_to_be_set': [],
        'dryrun': False,
        'append': False,
        'volumes_to_tag': "",
        'snapshots_to_tag': "",
        'instance_filter': [],
        'novolumes': False,
        'nosnapshots': False
    }

    if isinstance(event, dict):
        kwargs.update(event)

    monkey_lambda = GraffitiMonkey(**kwargs)
    monkey_lambda.propagate_tags()


if __name__ == '__main__':
    lambda_handler({'dryrun': True}, None)
