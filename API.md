# DubStack API reference


## POST /riak/login/

This endpoint authenticates user and returns bearer token with other details.


### Body

```json
{
   "login":"string",
   "password":"string"
}
```

### Success Response

**Code** : `200 OK`

### Other Response Codes

**Code** : `400 Bad Request` When incorrect JSON values provided


#### Request Example
```sh
curl -X POST "http://127.0.0.1/riak/login" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -d "{ \"login\": \"if@example.com\", \"password\": \"secret\" }"
```

#### Response Example
```json
{
   "id":"348e662e64af42631fc28285de8b7d72",
   "name":"Iван Франко",
   "tenant_id":"poetry",
   "tenant_name":"Поети",
   "tenant_enabled":"true",
   "login":"if@example.com",
   "enabled":"true",
   "staff":"true",
   "groups":[
      {
         "id":"naukovtsi",
         "name":"Науковцi",
         "available_bytes":-1,
         "bucket_id":"the-poetry-naukovtsi-res",
         "bucket_suffix":"res"
      },
      {
         "id":"poety",
         "name":"Поети",
         "available_bytes":-1,
         "bucket_id":"the-poetry-poety-res",
         "bucket_suffix":"res"
      }
   ],
   "token":"a39b7220-1379-40ac-8aca-266477b28b77"
}
```

**token** : This value should be specified in all further requests. For example:
```sh
curl -H "authorization : Token $TOKEN" \
..
```

GET /riak/logout/

Removes bearer token or session id, if the last one is submitted in headers by browser.

### Success Response

**Code** : `200 OK`

**Code** : `302 Redirect` to login, if session id was provided


#### Request Example

```sh
curl -v -X GET $URL/riak/logout/ \
    -H "authorization: Token $TOKEN"
```

## GET /riak/list/[:bucket_id] 

Use this API endpoint to get the list of objects. 
It returns contents of cached index, containing list of objects and pseudo-directories.

### Parameters

**prefix** : Hex-encoded UTF8 string. For example "blah" becomes ```"626c6168"```.

**Auth required** : YES


### Success Response

**Code** : `200 OK`

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found


#### Request Example
```sh
curl -vv -X GET "http://127.0.0.1/riak/list/the-poetry-naukovtsi-res/?prefix=64656d6f/" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN"
```

#### Response Example
```json
{
   "list":[
      {
         "object_key":" img_20180225_130754.jpg",
         "orig_name": "IMG_20180225_130754.jpg",
         "bytes": 2690467,
         "content_type": "image/jpeg",
         "last_modified_utc": 1539366733,
         "guid": "c699175a-9f58-44b0-83f8-18e166b12b0d",
         "author_id": "706f77d2b233c2a1935b5473edb764d2",
         "author_name": "Іван Франко",
         "author_tel": "0951234567 (фейк)",
         "is_locked": true,
         "lock_user_id": "706f77d2b233c2a1935b5473edb764d2",
         "lock_user_name": "Іван Франко",
         "lock_user_tel": "0951234567 (фейк)",
         "lock_modified_utc": 1580838703,
         "is_deleted": false,
         "md5": "9ae51374bb6623ec3b6d17a5c2415391-2",
         "width": 1024,
         "height": 768
      }
   ],
   "dirs":[
      {
         "prefix":"64656d6f/74657374/",
         "bytes":0,
         "is_deleted":false
      }
   ],
   "uncommitted": false
}
```


## POST /riak/list/[:bucket_id]

This API endpoint creates pseudo-directory, that is stored as Hex-encoded value of UTF8 string.

### Body

```json
{
   "directory_name":"string",
   "prefix":"string"
}
```

**Auth required** : YES

### Success Response

**Code** : `204 No Content`

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` When incorrect JSON values provided

**Code** : `429 Too Many Requests` When server is unable to process request, as Riak CS is overloaded.


#### Request Example
```sh
curl -X POST "http://127.0.0.1/riak/list/the-poetry-naukovtsi-res" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"prefix\": \"64656d6f/\", \"directory_name\": \"blah\" }"
```

**prefix** : Hex-encoded UTF8 string. For example "blah" becomes ```"626c6168"```.

**directory_name** : UTF8 name of new pseudo-dorectory, that should be created.


## PATCH /riak/list/[:bucket_id]

This API andpoint allows to *lock*, *unlock*, *undelete* objects.
Undelete operation marks objects as visible again.
Lock marks them immutable and unlock reverses that operation.

### Body

```json
{
   "op": "undelete",
   "prefix": "string",
   "objects": ["string", "string", .. "string"]
}

{
   "op": "lock",
   "prefix": "string",
   "objects": ["string", "string", .. "string"]
}

{
   "op": "unlock",
   "prefix": "string",
   "objects": ["string", "string", .. "string"]
}
```

**Auth required** : YES

### Success Response

**Code** : `204 No Content`

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` When incorrect JSON values provided

**Code** : `429 Too Many Requests` When server is unable to process request, as Riak CS is overloaded.


#### Request Example
```sh
curl -X PATCH "http://127.0.0.1/riak/list/the-poetry-naukovtsi-res" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"prefix\": \"64656d6f/\", \"object_key\": \"something.random\" }"
```

## DELETE /riak/list/[:bucket_id]

Marks objects as deleted. In case of pseudo-directoies, it renames them and makrs them as deleted.

### Body

```json
{
   "object_keys": ["string", "string", ..],
   "prefix": "string"
}
```

In order to delete pseudo-directory, its name should be encoded as hex value 
and passed as "object_key" with "/" at the end. For example 

```json
{
   "object_keys": ["64656d6f/", "something.jpg"] ,
   "prefix": "74657374/"
}
```

**Auth required** : YES

### Success Response

**Code** : `200 OK`

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` When incorrect JSON values provided

**Code** : `429 Too Many Requests` When server is unable to process request, as Riak CS is overloaded.


#### Request Example
```sh
curl -X DELETE "http://127.0.0.1/riak/object/the-poetry-naukovtsi-res" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"object_key\": \"something.jpg\" }"
```

#### Response Example

```json
{
   "status": "ok"
}
```

## GET /riak/thumbnail/[:bucket_id]

Generate image thumbnail. Scales image, stored in Riak CS to width or heigt, specified in request
Returns binary image data.

### Parameters

**prefix** : Hex-encoded UTF8 string. For example "blah" becomes "626c6168".

**object_key** : ASCII "object_key" value returned by GET /riak/list/[:bucket_id] API endpoint.

**w** : Requested width (optional)

**h*** : Requested height (optional)

**dummy** : If dummy=1 and image not found, returns image with text "image unavailable".

**crop** : Boolean flag, telling server to crop image during scaling. Default: 1 (true)


**Auth required** : YES

### Success Response

**Code** : `200 OK`

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` When incorrect JSON values provided


#### Request Example
```sh
curl -X GET "http://127.0.0.1/riak/thumbnail/the-poetry-naukovtsi-res" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"object_key\": \"something.jpg\" }"
```

It returns binary image data.


## POST /riak/upload/[:bucket_id]

Allows to upload files to Riak CS. 
It also do the following
- Creates bucket if it do not exist
- Transliterates provided filename for use in URL
- Attaches metadata to uploaded file
- Updates cached index ( list of files ) and full-text-search index

If file is bigger than ``FILE_UPLOAD_CHUNK_SIZE``, it returns upload ID, that should be used
to upload next parts of file.

Upload operation places file to special prefix, ``RIAK_REAL_OBJECT_PREFIX``, configured in riak.hrl
and then puts link to the real object in requested pseudo-directory. This allows to perform file operations,
such as **copy**, **move**, **rename**, **delete**, **undelete** faster.

**Auth required** : YES

### Success Response

**Code** : `100 Continue`

**Code** : `200 OK`


### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers.

**Code** : `403 Forbidden` When user has no access to bucket.

**Code** : `404 Not Found` When prefix not found.

**Code** : `400 Bad Request` In case of incorrect headers or multipart field values.

**Code** : `304 Not Modified` When modified_utc is less than or equal to existing object on the server.

**Code** : `429 Too Many Requests` When server is unable to process request, as Riak CS is overloaded.


#### Request Example
```sh
FILE_SIZE_RANGE_TOTAL=`stat --printf="%s" something.random`
let "FILE_SIZE_RANGE_END=$FILE_SIZE_RANGE_TOTAL - 1"
if uname | grep -q "Darwin"; then
    time_fmt="-f %m"
else
    time_fmt="-c %Y"
fi
TIMESTAMP=`stat $time_fmt something.random`
MD5SUM=`md5sum something.random | awk '{print $1}'`

curl -v -X POST "http://127.0.0.1/riak/upload/the-poetry-naukovtsi-res/" \
    -F "modified_utc=$TIMESTAMP" \
    -F "last_seen_modified_utc=$TIMESTAMP" \
    -F "etags[]=$MD5SUM" \
    -F "files[]=@something.random;filename=Something.random" \
    -H "accept: application/json" \
    -H "content-range: bytes 0-$FILE_SIZE_RANGE_END/$FILE_SIZE_RANGE_TOTAL" \
    -H "authorization: Token $TOKEN"
```
**content-range** : Format of this header is the following.
```
start byte-end byte/total bytes. 
```
It is optional for small files under `FILE_UPLOAD_CHUNK_SIZE` bytes.

**modified_utc** : Last modification file. This parameter is stored in metadata and used to check if next upload with the same filename should replace existing file or not.

**last_seen_modified_utc** : Tells server the time when client have seen updates last time. This is optional parameter.

**etags[]** : Md5 sum of binary part of data

#### Response Example
```json
      {
         "object_key":" "something.random",
         "orig_name": "Something.random",
         "bytes": 2690467,
         "content_type": "application/octet-stream",
         "last_modified_utc": 1539366733,
         "guid": "c699175a-9f58-44b0-83f8-18e166b12b0d",
         "author_id": "706f77d2b233c2a1935b5473edb764d2",
         "author_name": "Іван Франко",
         "author_tel": "0951234567 (фейк)",
         "is_locked": true,
         "lock_user_id": "706f77d2b233c2a1935b5473edb764d2",
         "lock_user_name": "Іван Франко",
         "lock_user_tel": "0951234567 (фейк)",
         "lock_modified_utc": 1580838703,
         "is_deleted": false,
         "md5": "9ae51374bb6623ec3b6d17a5c2415391-2",
         "width": null,
         "height": null
      }

// In case of big file, the response to the first upload would look the following way:
{
    "upload_id":"Z7mFJgyZRQ6G8xxNciYp1g",
    "last_modified_utc":1510914514,
    "end_byte":1999999,
    "md5":"c168dbdff4b1ba42392ec0634254d5f6",
    "guid":"4ea476c4-50d1-48e6-aefc-30bfebecb347"
}
```

## POST /riak/upload/[:bucket_id]/[:upload_id]/[:part_num]/

This API endpint should be used to continue upload of big file.


**Auth required** : YES

### Success Response

**Code** : `100 Continue`

**Code** : `200 OK`


### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` In case of incorrect headers or multipart field values


#### Request Example
```sh
#
# The default chunk size is 2 MB
#
CHUNK_SIZE=2000000
FILE_SIZE_RANGE_TOTAL=`stat --printf="%s" something.random`
#
# Splitting file to 2 MB chunks
#
split -b $CHUNK_SIZE something.random

#
# Calculate content-range header values
#
FIRST_CHUNK_TOTAL=$CHUNK_SIZE
let "FIRST_CHUNK_RANGE_END=$CHUNK_SIZE-1"
let "SECOND_CHUNK_RANGE_END=$FILE_SIZE_RANGE_TOTAL-1"
if uname | grep -q "Darwin"; then
    time_fmt="-f %m"
else
    time_fmt="-c %Y"
fi
TIMESTAMP=`stat $time_fmt xaa`

#
# Upload first chunk
#
FIRST_CHUNK_STATUS=`curl -s -X POST "http://127.0.0.1/riak/upload/the-greenhouse-test-res/" \
    -F 'files[]=@xaa;filename=something.random' \
    -F "object_name=Something.random" \
    -F "modified_utc=$TIMESTAMP" \
    -H "accept: application/json" \
    -H "content-range: bytes 0-$FIRST_CHUNK_RANGE_END/$FILE_SIZE_RANGE_TOTAL" \
    -H "authorization: Token $TOKEN"`

#
# Parse Upload ID
#
UPLOAD_ID=`echo $FIRST_CHUNK_STATUS|python -c "import sys, json; print json.load(sys.stdin)['upload_id']"`
GUID=`echo $FIRST_CHUNK_STATUS|python -c "import sys, json; print json.load(sys.stdin)['guid']"`
FIRST_END_BYTE=`echo $FIRST_CHUNK_STATUS|python -c "import sys, json; print json.load(sys.stdin)['end_byte']"`
FIRST_MD5=`echo $FIRST_CHUNK_STATUS|python -c "import sys, json; print json.load(sys.stdin)['md5']"`
SECOND_MD5=`md5sum xab| awk '{ print $1 }'`

let "SECOND_START_BYTE=$FIRST_END_BYTE+1"

curl -s -X POST "$URL/riak/upload/the-greenhouse-test-res/$UPLOAD_ID/2/" \
    -F 'files[]=@xab;filename=something.random' \
    -F "guid=$GUID" \
    -F "etags[]=1,$FIRST_MD5,2,$SECOND_MD5" \
    -F "modified_utc=$TIMESTAMP" \
    -H "accept: application/json" \
    -H "content-range: bytes $SECOND_START_BYTE-$SECOND_CHUNK_RANGE_END/$FILE_SIZE_RANGE_TOTAL" \
    -H "authorization: Token $TOKEN"
```

#### Response Example
```json
{
    "upload_id":"Z7mFJgyZRQ6G8xxNciYp1g",
    "last_modified_utc":1510914514,
    "end_byte":1999999,
    "md5":"c168dbdff4b1ba42392ec0634254d5f6",
    "guid":"4ea476c4-50d1-48e6-aefc-30bfebecb347"
}
```


## POST /riak/copy/[:src_bucket_id]/

Copy object or directory.


**Auth required** : YES

### Success Response

**Code** : `200 OK`

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` In case of incorrect headers or multipart field values

**Code** : `202 Accepted` Application returns 202 when it failed to copy some of objects, or it failed to update index.
                          In that case response should contain the list of copied objects. List might be empty or
                          it can be incomplete. So client application should retry copy of those object that are
                          missing in the list.


### Body

```json
{
   "src_prefix": "string",    // Source prefix
   "dst_prefix": "string",    // Destination prefix
   "dst_bucket_id": "string", // Destination bucket
   "src_object_keys": {"key 1": "Destination Name 1", "key 2": "Destination Name 2"],
}
```

Response Example:
[{
    bytes: 20,
    src_orig_name: "Something.random",
    dst_orig_name: "Something.random",
    old_key: "something.random",
    new_key: "something.random",
    dst_prefix: "74657374/",
    guid: "6caef57f-fc6d-457d-b2b0-210a1ed2f753",
    renamed: false,
    src_prefix: null
}, ..]


#### Request Example
```sh
#
# The following request copies file "something.random" from pseudo-directory "demo" to "demo/test/".
#
curl -s -X POST "http://127.0.0.1/riak/copy/the-poetry-naukovtsi-res/" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"src_prefix\": \"64656d6f/\", \"dst_prefix\": \"64656d6f/74657374/\", \"dst_bucket_id\": \"the-poetry-naukovtsi-res\", \"src_object_keys\": [\"something.random\"] }"
```



## POST /riak/move/[:src_bucket_id]/

Move object or directory.

**Auth required** : YES

### Success Response

**Code** : `204 No Content`

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` In case of incorrect headers or multipart field values

**Code** : `202 Accepted` Application returns 202 when it failed to copy some of objects, or it failed to update index.
                          In that case response should contain the list of copied objects. List might be empty or
                          it can be incomplete. So client application should retry copy of those object that are 
                          missing in the list.

### Body

```json
{
   "src_object_keys":["string 1", "string 2"],
   "dst_bucket_id":"string",
   "dst_prefix":"string",
   "src_prefix":"string"
}
```

#### Request Example
```sh
#
# The following request moves file "something.random" from pseudo-directory "demo" to "demo/test/".
#
curl -s -X POST "http://127.0.0.1/riak/move/the-poetry-naukovtsi-res/" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"src_prefix\": \"64656d6f/\", \"dst_prefix\": \"64656d6f/74657374/\", \"dst_bucket_id\": \"the-poetry-naukovtsi-res\", \"src_object_keys\": [\"something.random\"] }"
```


## POST /riak/rename/[:src_bucket_id]/ 

Renames object or directory. Changes ``"orig_name"`` meta tag when called on object.
Moves nested objects to new prefix when used on pseudo-directories.

**Auth required** : YES

### Success Response

**Code** : `204 No Content`

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` In case of incorrect headers or multipart field values

**Code** : `202 Accepted` Application returns 202 when it failed to rename some of objects, or it failed to update index.
                          In that case response should contain the list of renamed objects. List might be empty or
                          it can be incomplete. So client application should retry finish rename manually in that case
                          Example of response body in that case: {"dir_errors": ["64656d6f/], "object_errors": []}

### Body

```json
{
   "src_object_key":"string",
   "dst_object_name":"string",
   "prefix":"string"
}
```

`src_object_key` is an object's **key**, but dst_object_name is UTF8 name.

#### Request Example
```sh
#
# The following request adds record to the .riak_index.etf (default)
# that the object "something-random.jpg" was renamed to "Something Something.jpg".
#
# Object key remains the same, but its orig_name meta tag changes.
#
curl -s -X POST "http://127.0.0.1/riak/rename/the-poetry-naukovtsi-res/" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"prefix\": \"64656d6f/\", \"src_object_key\": \"something-random.jpg\", \"dst_object_name\": \"Something Something.jpg\" }"
```

## GET /riak/download/[...]

Allows to download file, in case user belongs to group where file is stored.

**Auth required** : YES


#### Request Example
```sh
curl "http://127.0.0.1/riak/download/the-poetry-naukovtsi-res/d0bfd180d0b8d0bad0bbd0b0d0b4/something.random" \
    -H "authorization: Token $TOKEN" --output something.random
```

### Success Response

**Code** : `200 OK`

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` When incorrect JSON values provided


## GET /riak/action-log/[:bucket_id]/

Fetch the history of specified pseudo-directory.

### Parameters

**prefix** : Hex-encoded UTF8 string. For example "blah" becomes ```"626c6168"```.

**object_key** : The key that upload/list returns for referencing object in URL.
		 If provided, the object history of changes will be returned.

**Auth required** : YES

#### Response Example
```json
{
    "action": "copy",
    "details": "Copied from "/".",
    "tenant_name": "Poets",
    "timestamp": "1575107875",
    "user_name": "Іван Франко"
}
```


## POST /riak/action-log/[:bucket_id]/

Allow to restore previous version of file.

### Parameters

**prefix** : Hex-encoded UTF8 string. For example "blah" becomes ```"626c6168"```.


**Auth required** : YES

#### Request Example
```sh
curl -s -X POST "http://127.0.0.1/riak/action-log/?prefix=626c6168" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"object_key\": \"something-random.jpg\", \"timestamp\": \"1576148851\" }"
```

### Success Response

**Code** : `200 OK`
