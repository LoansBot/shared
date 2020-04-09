"""A simple wrapper around the ArangoDB's HTTP interface to support basic CRUD
operations."""
import requests
from dataclasses import dataclass
import base64
import typing


class Cluster:
    def __init__(self, urls: typing.List[str]):
        self.urls = urls
        self._last_server_ind = 0

    def select_url(self):
        res = self.urls[self._last_server_ind]
        self._last_server_ind = (self._last_server_ind + 1) % len(self.urls)
        return res

    def select_url_for_endpoint(self, endpoint):
        return f'{self.select_url()}{endpoint}'


class BasicAuth:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.authorization_header = 'Basic ' + base64.b64encode(f'{username}:{password}')


def standard_headers(auth: BasicAuth):
    return {
        'Authorization': auth.authorization_header,
        'Accept': 'application/json'
    }


# https://www.arangodb.com/docs/stable/http/database-database-management.html#create-database
def create_database(cluster: Cluster, auth: BasicAuth, **args: dict):
    return requests.post(
        cluster.select_url_for_endpoint('/_db/_system/_api/database'),
        headers=standard_headers(auth),
        json=args
    )


# https://www.arangodb.com/docs/stable/http/collection-creating.html#create-collection
def create_collection(
        cluster: Cluster, auth: BasicAuth, database: str, **args):
    return requests.post(
        cluster.select_url_for_endpoint(f'/_db/{database}/_api/collection'),
        headers=standard_headers(auth),
        json=args
    )


# https://www.arangodb.com/docs/stable/http/document-working-with-documents.html#create-document
def create_document(
        cluster: Cluster, auth: BasicAuth,
        database: str, collection: str, document: dict, **args):
    return requests.post(
        cluster.select_url_for_endpoint(f'/_db/{database}/_api/collection'),
        headers=standard_headers(auth),
        params=args,
        json=document
    )


# https://www.arangodb.com/docs/stable/http/database-database-management.html#list-of-accessible-databases
def list_databases(cluster: Cluster, auth: BasicAuth):
    return requests.get(
        cluster.select_url_for_endpoint('/_db/_system/_api/database/user'),
        headers=standard_headers(auth)
    )


# https://www.arangodb.com/docs/stable/http/collection-getting.html#return-information-about-a-collection
def read_collection(cluster: Cluster, auth: BasicAuth, database: str, collection: str):
    return requests.get(
        cluster.select_url_for_endpoint(f'/_db/{database}/_api/collection/{collection}'),
        headers=standard_headers(auth)
    )


# https://www.arangodb.com/docs/stable/http/document-working-with-documents.html#read-document
def read_document(
        cluster: Cluster, auth: BasicAuth, database: str, collection: str, key: str, etag: str):
    if etag is None:
        headers = standard_headers(auth)
    else:
        headers = {
            'If-None-Match': etag,
            **standard_headers(auth)
        }
    return requests.get(
        cluster.select_url_for_endpoint(f'/_db/{database}/_api/document/{collection}/{key}'),
        headers=headers
    )


# https://www.arangodb.com/docs/stable/http/document-working-with-documents.html#replace-document
def replace_document(
        cluster: Cluster, auth: BasicAuth,
        database: str, collection: str, document: dict, etag: str, **args):
    if etag is None:
        headers = standard_headers(auth)
    else:
        headers = {'If-Match': etag, **standard_headers(auth)}
    return requests.put(
        cluster.select_url_for_endpoint(
            f'/_db/{database}/_api/document/{collection}/{document["_key"]}'),
        headers=headers,
        params=args,
        json=document
    )


# https://www.arangodb.com/docs/stable/http/collection-creating.html#drops-a-collection
def delete_collection(cluster: Cluster, auth: BasicAuth, database: str, collection: str, **args):
    return requests.delete(
        cluster.select_url_for_endpoint(
            f'/_db/{database}/_api/collection/{collection}'),
        headers=standard_headers(auth),
        params=args
    )


# https://www.arangodb.com/docs/stable/http/document-working-with-documents.html#removes-a-document
def delete_document(
        cluster: Cluster, auth: BasicAuth, database: str, collection: str,
        key: str, etag: str, **args):
    if etag is None:
        headers = standard_headers(auth)
    else:
        headers = {'If-Match': etag, **standard_headers(auth)}

    return requests.delete(
        cluster.select_url_for_endpoint(
            f'/_db/{database}/_api/document/{collection}/{key}',
            headers=headers,
            params=args
        )
    )

# Example:
# doc = Document(..., key='test')
# doc.body['foo'] = True
# doc.create(overwrite=True)
# doc.body['foo'] = False
# doc.save()
# doc2 = Document(..., key='test')
# doc2.read()
# assert doc2.body['foo'] is False
# doc.delete()
class Document:
    def __init__(
            self, cluster: Cluster, auth: BasicAuth, database: str, collection: str,
            key: str = None):
        self.cluster = cluster
        self.auth = auth
        self.database = database
        self.collection = collection
        self.body = {}
        self.key = key
        self.rev = None
        self.etag = None

    def read(self, try_304=True):
        assert self.key is not None

        result = read_document(
            self.cluster, self.auth, self.database, self.collection,
            self.key, self.etag if try_304 else None
        )
        if result.status_code == 304:
            return True, result
        if result.status_code != 200:
            return False, result
        body = result.json()
        self.key = body.pop('_key')
        self.rev = body.pop('_rev')
        body.pop('_id')
        self.body = body
        self.etag = result.headers['etag']
        return True, result

    def create(self, overwrite=False):
        result = create_document(
            self.cluster, self.auth, self.database, self.collection,
            {'_key': self.key, **self.body},
            overwrite=overwrite
        )
        if result.status_code != 201 and result.status_code != 202:
            return False, result
        body = result.json()
        self.key = body['_key']
        self.rev = body['_rev']
        self.etag = result.headers['etag']
        return True, result

    def save(self):
        assert self.key is not None
        assert self.rev is not None
        assert self.etag is not None
        result = replace_document(
            self.cluster, self.auth, self.database, self.collection,
            {'_key': self.key, **self.body}, self.etag
        )
        if result.status_code != 201 and result.status_code != 202:
            return False, result
        body = result.json()
        self.rev = body['_rev']
        self.etag = result.headers['etag']
        return True, result

    def delete(self, ignore_revision=False, treat_404_as_success=True):
        assert self.key is not None
        assert ignore_revision or (self.etag is not None)
        result = delete_document(
            self.cluster, self.auth, self.database, self.collection,
            self.key, None if ignore_revision else self.etag
        )
        if (result.status_code == 200 or result.status_code == 202
                or (treat_404_as_success and result.status_code == 404)):
            self.rev = None
            self.etag = None
            return True, result
        return False, result


class Collection:
    def __init__(self, cluster: Cluster, auth: BasicAuth, database: str, collection: str):
        self.cluster = cluster
        self.auth = auth
        self.database = database
        self.collection = collection

    def create(self, **args):
        result = create_collection(
            self.cluster, self.auth, self.database, self.collection,
            {'name': self.collection, **args}
        )
        succ = result.status_code == 200
        return succ, result

    def delete(self):
        result = delete_collection(
            self.cluster, self.auth, self.database, self.collection
        )
        succ = result.status_code == 200 or result.status_code == 202
        return succ, result

    def new_document(self, key: str):
        return Document(
            self.cluster, self.auth, self.database, self.collection, None, key=key
        )

class Database:
    def __init__(self, cluster: Cluster, auth: BasicAuth, database: str):
        self.cluster = cluster
        self.auth = auth
        self.database = database

    def create(self, *args):
        result = create_database(
            self.cluster, self.auth, {'name': self.database, **args}
        )
        return result.status_code == 201, result

    def new_collection(self, collection: str):
        return Collection(
            self.cluster, self.auth, self.database, collection
        )


class Connection:
    def __init__(self, cluster: Cluster, auth: BasicAuth):
        self.cluster = cluster
        self.auth = auth

    def new_database(self, database: str):
        return Database(
            self.cluster, self.auth, database
        )

    def list_databases(self):
        result = list_databases(self.cluster, self.auth)
        succ = result.status_code == 200
        return succ, result
