"""Deterministic S3 accounting; LIST counts XML bytes, not stored object sizes."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import io
from xml.etree.ElementTree import Element, SubElement, tostring

from botocore.exceptions import ClientError

NOW = datetime(2026, 11, 1, tzinfo=timezone.utc)


class MeteredS3:
    def __init__(self):
        self.objects = {}
        self.reset()

    def reset(self):
        self.calls = Counter()
        self.bytes = Counter()

    def usage(self):
        return {'calls': dict(self.calls), 'bytes': dict(self.bytes)}

    def add(self, bucket, key, body=b'{}', modified=NOW, metadata=None):
        self.objects[bucket, key] = {
            'body': body, 'LastModified': modified,
            'ETag': '"' + hashlib.sha256(body).hexdigest() + '"',
            'Metadata': dict(metadata or {}),
        }

    def _object(self, bucket, key, operation):
        try:
            return self.objects[bucket, key]
        except KeyError:
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, operation) from None

    def list_objects_v2(self, Bucket, Prefix='', MaxKeys=1000, ContinuationToken=None, **kwargs):
        self.calls['LIST'] += 1
        keys = sorted(k for b, k in self.objects if b == Bucket and k.startswith(Prefix))
        start = int(ContinuationToken or 0)
        keys_page = keys[start:start + min(MaxKeys, 1000)]
        contents = []
        root = Element('ListBucketResult')
        for key in keys_page:
            item = self.objects[Bucket, key]
            row = {'Key': key, 'Size': len(item['body']), 'ETag': item['ETag'],
                   'LastModified': item['LastModified']}
            contents.append(row)
            node = SubElement(root, 'Contents')
            for name, value in row.items():
                SubElement(node, name).text = value.isoformat() if isinstance(value, datetime) else str(value)
        truncated = start + len(keys_page) < len(keys)
        SubElement(root, 'IsTruncated').text = str(truncated).lower()
        self.bytes['LIST'] += len(tostring(root, encoding='utf-8'))
        result = {'Contents': contents, 'IsTruncated': truncated}
        if truncated:
            result['NextContinuationToken'] = str(start + len(keys_page))
        return result

    def get_paginator(self, operation):
        assert operation == 'list_objects_v2'
        client = self

        class Paginator:
            def paginate(self, **kwargs):
                while True:
                    page = client.list_objects_v2(**kwargs)
                    yield page
                    if not page['IsTruncated']:
                        return
                    kwargs['ContinuationToken'] = page['NextContinuationToken']

        return Paginator()

    def head_object(self, Bucket, Key, **kwargs):
        self.calls['HEAD'] += 1
        item = self._object(Bucket, Key, 'HeadObject')
        self.bytes['HEAD'] += 512  # Conservative fixed metadata/header estimate.
        return {k: item[k] for k in ('ETag', 'LastModified', 'Metadata')} | {
            'ContentLength': len(item['body'])}

    def get_object(self, Bucket, Key, **kwargs):
        self.calls['GET'] += 1
        item = self._object(Bucket, Key, 'GetObject')
        if kwargs.get('IfMatch', item['ETag']) != item['ETag']:
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'GetObject')
        self.bytes['GET'] += len(item['body'])
        return {k: item[k] for k in ('ETag', 'LastModified', 'Metadata')} | {
            'ContentLength': len(item['body']), 'Body': io.BytesIO(item['body'])}

    def put_object(self, Bucket, Key, Body, Metadata=None, **kwargs):
        self.calls['PUT'] += 1
        self.add(Bucket, Key, bytes(Body), metadata=Metadata)

    def delete_object(self, Bucket, Key, **kwargs):
        self.calls['DELETE'] += 1
        self.objects.pop((Bucket, Key), None)

    def get_bucket_versioning(self, **kwargs):
        self.calls['VERSIONING'] += 1
        return {}


def assert_idle_budget(usage, *, context):
    """One small discovery/control response; no historical object revalidation."""
    calls, sizes = usage['calls'], usage['bytes']
    failures = []
    for method, limit in {'LIST': 1, 'HEAD': 6, 'GET': 1, 'PUT': 0, 'DELETE': 1, 'VERSIONING': 1}.items():
        if calls.get(method, 0) > limit:
            failures.append(f'{method}={calls[method]} > {limit}')
    if sum(sizes.values()) > 64 * 1024:
        failures.append(f'received={sum(sizes.values())} > 65536 bytes')
    assert not failures, f'{context}: idle S3 budget exceeded: {failures}; measured={usage}'


def assert_growth_budget(small, large, *, context):
    """History ×10 must not multiply steady-state requests/bytes by ×10."""
    failures = []
    for method in ('LIST', 'HEAD', 'GET', 'PUT', 'DELETE', 'VERSIONING'):
        before, after = small['calls'].get(method, 0), large['calls'].get(method, 0)
        if after > max(1, 2 * before):
            failures.append(f'{method}: {before} -> {after}')
    before, after = sum(small['bytes'].values()), sum(large['bytes'].values())
    if after > max(4096, 2 * before):
        failures.append(f'bytes: {before} -> {after}')
    assert not failures, f'{context}: history ×10 exceeds growth ×2: {failures}'
