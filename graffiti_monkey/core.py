# Copyright 2013 Answers for AWS LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from exceptions import *

import boto3
import botocore

import time

__all__ = ('GraffitiMonkey', 'Logging')
log = logging.getLogger(__name__)


class GraffitiMonkey(object):
    def __init__(self, region, profile, instance_tags_to_propagate, volume_tags_to_propagate, volume_tags_to_be_set, snapshot_tags_to_be_set, dryrun, append, volumes_to_tag, snapshots_to_tag, instance_filter, noamis, novolumes, nosnapshots):
        # This list of tags associated with an EC2 instance to propagate to
        # attached EBS volumes
        self._instance_tags_to_propagate = instance_tags_to_propagate

        # This is a list of tags associated with a volume to propagate to
        # a snapshot created from the volume
        self._volume_tags_to_propagate = volume_tags_to_propagate

        # This is a dict of tags (keys and values) which will be set on the volumes (ebs)
        self._volume_tags_to_be_set = volume_tags_to_be_set

        # This is a dict of tags (keys and values) which will be set on the snapshots
        self._snapshot_tags_to_be_set = snapshot_tags_to_be_set

        # The region to operate in
        self._region = region

        # The profile to use
        self._profile = profile

        # Whether this is a dryrun
        self._dryrun = dryrun

        # If we are appending tags
        self._append = append

        # Volumes we will tag
        self._volumes_to_tag = volumes_to_tag

        # Snapshots we will tag
        self._snapshots_to_tag = snapshots_to_tag

        # Filter instances by a given param and propagate their tags to their attached volumes
        self._instance_filter = instance_filter

        # If we propagate AMI information to snapshots
        self._noamis = noamis

        # If we process volumes
        self._novolumes = novolumes

        # If we process snapshots
        self._nosnapshots = nosnapshots

        log.info("Starting Graffiti Monkey")
        log.info("Options: dryrun %s, append %s, noamis %s, novolumes %s, nosnapshots %s", self._dryrun, self._append, self._noamis, self._novolumes, self._nosnapshots)
        log.info("Connecting to region %s using profile %s", self._region, self._profile)
        try:
            session = boto3.Session(profile_name=profile)
            self._conn = session.client('ec2', region_name=self._region)
        except botocore.exceptions.ProfileNotFound:
            raise GraffitiMonkeyException('No AWS credentials found - check your credentials')
        except (botocore.exceptions.NoCredentialsError, botocore.exceptions.PartialCredentialsError, botocore.exceptions.CredentialRetrievalError):
            log.info("Connecting to region %s using default credentials", self._region)
            try:
                session = boto3.Session()
                self._conn = session.client('ec2', region_name=self._region)
            except botocore.exceptions.ProfileNotFound:
                raise GraffitiMonkeyException('No AWS credentials found - check your credentials')


    def propagate_tags(self):
        ''' Propagates tags by copying them from EC2 instance to EBS volume, and
        then to snapshot '''

        amis = {}
        if not self._noamis:
            response = self._conn.describe_images(Owners=["self"])['Images']

            for image in response:
                for mapping in image['BlockDeviceMappings']:
                    if 'Ebs' in mapping.keys():
                        amis[mapping['Ebs']['SnapshotId']] = {
                           'Tags': [
                              {
                                  'Key': 'AMI',
                                  'Value': image['ImageId']
                              },
                              {
                                  'Key': 'AMI Name',
                                  'Value': image['Name']
                              },
                              {
                                  'Key': 'AMI Description',
                                  'Value': image['Description']
                              }
                           ]
                        }
                        if 'Tags' in image:
                            amis[mapping['Ebs']['SnapshotId']]['Tags'].extend(image['Tags'])

            log.info("Compiled AMI information for tagging.")

        volumes = []
        if not self._novolumes:
            volumes = self.tag_volumes()

        volumes = { v["VolumeId"]: v for v in volumes }

        if not self._nosnapshots:
            self.tag_snapshots(volumes, amis)

    def tag_volumes(self):
        ''' Gets a list of volumes, and then loops through them tagging
        them '''

        storage_counter = 0
        volumes = []
        instances = {}

        if self._volumes_to_tag:
            log.info('Using volume list from cli/config file')

            # Max of 200 filters in a request
            for chunk in (self._volumes_to_tag[n:n+200] for n in xrange(0, len(self._volumes_to_tag), 200)):

                params = {
                    'Filters': [
                        {
                            'Name': 'volume-id',
                            'Values': chunk
                        }
                    ]
                }
                paginator = self._conn.get_paginator('describe_volumes')
                chunk_volumes = sum([x['Volumes'] for x in paginator.paginate(**params)], [])
                volumes.extend(chunk_volumes)

                chunk_instance_ids = set([a['InstanceId']
                                          for v in chunk_volumes
                                          for a in v['Attachments']])

                paginator = self._conn.get_paginator('describe_instances')
                params = {
                    'Filters': [
                        {
                            'Name': 'instance-id',
                            'Values': [id for id in chunk_instance_ids]
                        }
                    ]
                }
                paginator = self._conn.get_paginator('describe_instances')
                instances = {i['InstanceId']: i
                             for p in paginator.paginate(**params)
                             for r in p['Reservations']
                             for i in r['Instances']}

            volume_ids = [v['VolumeId'] for v in volumes]

            ''' We can't trust the volume list from the config file so we
            test the status of each volume and remove any that raise an exception '''
            for volume_id in self._volumes_to_tag:
                if volume_id not in volume_ids:
                    log.info('Volume %s does not exist and will not be tagged', volume_id)
                    self._volumes_to_tag.remove(volume_id)

        elif self._instance_filter:
            log.info('Filter instances and retrieve volume ids')
            instance_params = {
                'Filters': [
                    {
                        'Name': k,
                        'Values': list(v)
                    }
                    for k, v in self._instance_filter.iteritems()
                ]
            }
            paginator = self._conn.get_paginator('describe_instances')
            instances = {i['InstanceId']: i
                         for p in paginator.paginate(**instance_params)
                         for r in p['Reservations']
                         for i in r['Instances']}

            instance_keys = instances.keys()
            for chunk in (instance_keys[n:n+200] for n in xrange(0, len(instance_keys), 200)):
                volume_params = {
                    'Filters': [
                        {
                            'Name': 'attachment.instance-id',
                            'Values': chunk
                        }
                    ]
                }
                volumes.extend(self._conn.describe_volumes(**volume_params)['Volumes'])

        else:
            log.info('Getting list of all volumes')
            paginator = self._conn.get_paginator('describe_volumes')
            volumes = sum([x['Volumes'] for x in paginator.paginate()], [])

            paginator = self._conn.get_paginator('describe_instances')
            instances = {i['InstanceId']: i
                         for p in paginator.paginate()
                         for r in p['Reservations']
                         for i in r['Instances']}

        if not volumes:
            log.info('No volumes found')
            return True

        log.debug('Volume list >%s<', volumes)
        total_vols = len(volumes)
        log.info('Found %d volume(s)', total_vols)
        this_vol = 0
        for volume in volumes:
            this_vol += 1
            storage_counter += volume["Size"]
            log.info ('Processing volume %d of %d total volumes', this_vol, total_vols)

            if volume["State"] != 'in-use':
                log.debug('Skipping %s as it is not attached to an EC2 instance, so there is nothing to propagate', volume["VolumeId"])
                continue

            for attempt in range(5):
                try:
                    self.tag_volume(volume, instances)
                except botocore.exceptions.ClientError as e:
                    log.error("While attempting to tag volume %s: %s", volume['VolumeId'], e)
                    break
                except (botocore.exceptions.EndpointConnectionError, botocore.exceptions.ConnectionClosedError) as e:
                    log.error("While attempting to tag volume %s: %s. Waiting %d seconds before retrying.", volume['VolumeId'], e, attempt)
                    time.sleep(attempt)
                else:
                    break
            else:
                log.error("While attempting to tag volume %s: %s. %d retries failed, continuing", volume['VolumeId'], e, attempt)
                continue

        log.info('Processed a total of {0} GB of AWS Volumes'.format(storage_counter))
        log.info('Completed processing all volumes')

        return volumes


    def tag_volume(self, volume, instances):
        ''' Tags a specific volume '''

        instance_id = "Unknown"
        device = "Unknown"
        if len(volume["Attachments"]) > 0:
            instance_id = volume["Attachments"][0].get("InstanceId", "Unknown")
            device = volume["Attachments"][0].get("Device", "Unknown")

        volume_tags = dict([(x['Key'], x['Value']) for x in volume.get('Tags', [])])

        if self._append:
            tags_to_set = volume_tags.copy()
        else:
            tags_to_set = {}

        tags_to_set.update(dict([(x['Key'], x['Value']) for x in instances.get(instance_id, {}).get('Tags', [])
                                 if x['Key'] in self._instance_tags_to_propagate]))

        # Additional tags
        tags_to_set.update({
            'instance_id': instance_id,
            'device': device
        })

        # Set default tags for volume
        log.debug('Trying to set default tags: %s', self._volume_tags_to_be_set)
        tags_to_set.update(self._volume_tags_to_be_set)

        if volume_tags != tags_to_set:
            self._set_resource_tags(volume, 'VolumeId', tags_to_set)
        else:
            log.info('Volume "%s" already has the desired tags.', volume['VolumeId'])

        return True


    def tag_snapshots(self, volumes, amis):
        ''' Gets a list of snapshots, and then loops through them tagging
        them '''

        snapshots = []
        if self._snapshots_to_tag:
            log.info('Using snapshot list from cli/config file')

            # Max of 200 filters in a request
            for chunk in (self._snapshots_to_tag[n:n+200] for n in xrange(0, len(self._snapshots_to_tag), 200)):
                snapshots.extend(self._conn.describe_snapshots(SnapshotIds=self._snapshots_to_tag)['Snapshots'])
            snapshot_ids = [s['SnapshotId'] for s in snapshots]

            ''' We can't trust the snapshot list from the config file so we
            test the status of each and remove any that raise an exception '''
            for snapshot_id in self._snapshots_to_tag:
                if snapshot_id not in snapshot_ids:
                    log.info('Snapshot %s does not exist and will not be tagged', snapshot_id)
                    self._snapshots_to_tag.remove(snapshot_id)
        else:
            log.info('Getting list of all snapshots')
            kwargs = {"OwnerIds": ["self"]}
            paginator = self._conn.get_paginator('describe_snapshots')
            snapshots.extend(sum([s['Snapshots'] for s in paginator.paginate(**kwargs)], []))

        if not snapshots:
            log.info('No snapshots found')
            return True

        all_volume_ids = set(s["VolumeId"] for s in snapshots)
        extra_volume_ids = [id for id in all_volume_ids if id not in volumes]

        ''' Fetch any extra volumes that weren't carried over from tag_volumes() (if any) '''
        for chunk in (extra_volume_ids[n:n+200] for n in xrange(0, len(extra_volume_ids), 200)):
            extra_volumes = self._conn.describe_volumes(
                    Filters=[{"Name": "volume-id", "Values": chunk}]
                    )
            for vol in extra_volumes["Volumes"]:
                volumes[vol["VolumeId"]] = vol

        log.debug('Snapshot list >%s<', snapshots)
        total_snaps = len(snapshots)
        log.info('Found %d snapshot(s)', total_snaps)
        this_snap = 0

        for snapshot in snapshots:
            this_snap += 1
            log.info ('Processing snapshot %d of %d total snapshots', this_snap, total_snaps)
            for attempt in range(5):
                try:
                    self.tag_snapshot(snapshot, volumes, amis)
                except botocore.exceptions.ClientError as e:
                    log.error("Encountered Error %s on snapshot %s", e.error_code, snapshot.id)
                    break
                except (botocore.exceptions.EndpointConnectionError, botocore.exceptions.ConnectionClosedError) as e:
                    log.error("Encountered Error %s on snapshot %s, waiting %d seconds then retrying", e.error_code, snapshot.id, attempt)
                    time.sleep(attempt)
                else:
                    break
            else:
                log.error("Encountered Error %s on snapshot %s, %d retries failed, continuing", e.error_code, snapshot.id, attempt)
                continue
        log.info('Completed processing all snapshots')

    def tag_snapshot(self, snapshot, volumes, amis):
        ''' Tags a specific snapshot '''

        volume_id = snapshot["VolumeId"]

        if volume_id not in volumes:
            log.info("Snapshot %s volume %s not found. Snapshot will not be tagged", snapshot["SnapshotId"], volume_id)
            return

        snapshot_tags = dict([(x['Key'], x['Value']) for x in snapshot.get('Tags', [])])

        if self._append:
            tags_to_set = snapshot_tags.copy()
        else:
            tags_to_set = {}

        # update tags_to_set with AMI information if present
        ami_tags = {}
        if snapshot["SnapshotId"] in amis:
            ami_object = amis[snapshot["SnapshotId"]]

            ami_tags = dict([(x['Key'], x['Value']) for x in ami_object.get('Tags', [])])

            if len(ami_tags.keys()) > 0:
                tags_to_set.update(ami_tags)

        tags_to_set.update(dict([(x['Key'], x['Value']) for x in volumes[volume_id].get('Tags', [])
                                 if x['Key'] in self._volume_tags_to_propagate]))
        tags_to_set.update(self._snapshot_tags_to_be_set)

        if tags_to_set != snapshot_tags:
            self._set_resource_tags(snapshot, "SnapshotId", tags_to_set)
        else:
            log.info('Snapshot "%s" already has the desired tags.', snapshot['SnapshotId'])


    def _set_resource_tags(self, resource, resource_id, tags):
        ''' Sets the tags on the given AWS resource '''

        # Convert from dict to boto3 Key/Value list
        tags = [{'Key': k, 'Value': v} for (k, v) in tags.items()]
        if self._dryrun:
            log.info('DRYRUN: %s would have been tagged %s', resource[resource_id], tags)
        else:
            log.info('Tagging %s with [%s]', resource[resource_id], tags)
            self._conn.create_tags(Resources=[resource[resource_id]], Tags=tags)
            resource['Tags'] = tags


class Logging(object):
    # Logging formats
    _log_simple_format = '%(asctime)s [%(levelname)s] %(message)s'
    _log_detailed_format = '%(asctime)s [%(levelname)s] [%(name)s(%(lineno)s):%(funcName)s] %(message)s'

    def configure(self, verbosity = None):
        ''' Configure the logging format and verbosity '''

        # Configure our logging output
        if verbosity >= 2:
            logging.basicConfig(level=logging.DEBUG, format=self._log_detailed_format, datefmt='%Y-%m-%d %H:%M:%S')
        elif verbosity >= 1:
            logging.basicConfig(level=logging.INFO, format=self._log_detailed_format, datefmt='%Y-%m-%d %H:%M:%S')
        else:
            logging.basicConfig(level=logging.INFO, format=self._log_simple_format, datefmt='%Y-%m-%d %H:%M:%S')

        # Configure Boto's logging output
        if verbosity >= 4:
            logging.getLogger('boto3').setLevel(logging.DEBUG)
        elif verbosity >= 3:
            logging.getLogger('boto3').setLevel(logging.INFO)
        else:
            logging.getLogger('boto3').setLevel(logging.CRITICAL)
