# DubStack API reference

## POST /riak/login/ Login

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

### Other Response Codes

**Code** : `400 Bad Request` When incorrect JSON values provided



## GET /riak/list/[:bucket_id] List of objects in bucket

Returns contents of cached index, containing list of objects and pseudo-directories.

### Parameters

**prefix** : Hex-encoded UTF8 string. For example "blah" becomes ```"626c6168"```.

**Auth required** : YES


### Success Response

**Code** : `200 OK`

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
         "object_name":"img_20180225_130754.jpg",
         "orig_name":"IMG_20180225_130754.jpg",
         "bytes":2690467,
         "content_type":"image/jpeg",
         "last_modified":1539366733,
         "is_deleted":false,
         "md5":"9ae51374bb6623ec3b6d17a5c2415391-2",
         "access_token":"2nshwtcu4f2k73h5kh2x"
      }
   ],
   "dirs":[
      {
         "prefix":"64656d6f/74657374/",
         "bytes":0,
         "is_deleted":false
      }
   ]
}
```

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found



## POST /riak/list/[:bucket_id] Creates pseudo-directory

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


### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` When incorrect JSON values provided



## PATCH /riak/list/[:bucket_id] Undelete objects

Marks objects as visible again.

### Body

```json
{
   "undelete":"string",
   "prefix":"string"
}
```

**Auth required** : YES

### Success Response

**Code** : `204 No Content`

#### Request Example
```sh
curl -X PATCH "http://127.0.0.1/riak/list/the-poetry-naukovtsi-res" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"prefix\": \"64656d6f/\", \"object_name\": \"something.random\" }"
```

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` When incorrect JSON values provided



## GET /riak/object/[:bucket_id] Get object details

Returns object properties.

### Parameters

**object_name** : Object key in Riak CS

**prefix** : Hex-encoded UTF8 string. For example "blah" becomes ```"626c6168"```.

**Auth required** : YES

### Success Response

**Code** : `200 OK`

#### Request Example
```sh
curl -X GET "http://127.0.0.1/riak/object/the-poetry-naukovtsi-res" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"object_name\": \"something.jpg\" }"
```

#### Response Example

```json
{
   "prefix":"64656d6f/",
   "key":"something.jpg",
   "orig_name":"Something Something.jpg",
   "last_modified_utc":1534011746,
   "uploaded":"Wed, 17 Oct 2018 14:16:00 GMT",
   "content_length":66406,
   "content_type":"image/jpeg",
   "access_token":"eagwasuxts1uvcf8f4e0"
}
```

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` When incorrect JSON values provided



## DELETE /riak/object/[:bucket_id] Delete object or pseudo-directory

Marks object as deleted. Such objects can be removed with s3cmd by cron.

### Body

```json
{
   "object_name":"string",
   "prefix":"string"
}
```

In order to delete pseudo-directory, its name should be encoded as hex value 
and passed as "object_name" with "/" at the end. For example 

```json
{
   "object_name": "64656d6f/",
   "prefix": "74657374"
}
```

**Auth required** : YES

### Success Response

**Code** : `200 OK`

#### Request Example
```sh
curl -X DELETE "http://127.0.0.1/riak/object/the-poetry-naukovtsi-res" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"object_name\": \"something.jpg\" }"
```

#### Response Example

```json
{
   "status": "ok"
}
```

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` When incorrect JSON values provided



## GET /riak/thumbnail/[:bucket_id] Generate image thumbnail

Scales image, stored in Riak CS to provided width or heigt and returns binary image data.

### Parameters

**prefix** : Hex-encoded UTF8 string. For example "blah" becomes "626c6168".

**object_name** : ASCII "object_name" value returned by GET /riak/list/[:bucket_id] API endpoint.

**w** : Width of thumbnail

**h*** : Height of thumbnail

**access_token** : Secret token you can use for sharing output image.

**dummy** : If dummy=1 and image not found, returns image with text "image unavailable".


**Auth required** : YES

### Success Response

**Code** : `200 OK`

#### Request Example
```sh
curl -X GET "http://127.0.0.1/riak/thumbnail/the-poetry-naukovtsi-res" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"object_name\": \"something.jpg\" }"
```

It returns binary image data.

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` When incorrect JSON values provided



## POST /riak/upload/[:bucket_id] Upload file

Allows to upload files to Riak CS. This API endpoint also creates bucket if it do not exist,
transliterates provided ``"object_name"`` to use it as Riak CS object key, attaches original
name and modification time to object metadata, so desktop applications can find what version
of file is latest.

If file is bigger than ``FILE_UPLOAD_CHUNK_SIZE``, it returns upload ID, that should be used
to upload next parts of file.


**Auth required** : YES

### Success Response

**Code** : `100 Continue`

**Code** : `200 OK`


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

curl -v -X POST "http://127.0.0.1/riak/upload/the-poetry-naukovtsi-res/" \
    -F "files[]=@something.random;filename=Something.random" \
    -F "object_name=Something.random" \
    -F "modified_utc=$TIMESTAMP" \
    -H "accept: application/json" \
    -H "content-range: bytes 0-$FILE_SIZE_RANGE_END/$FILE_SIZE_RANGE_TOTAL" \
    -H "authorization: Token $TOKEN"
```
**content-range** : Format of this header is start byte-end byte/total bytes. It is optional for small files under `FILE_UPLOAD_CHUNK_SIZE` bytes.

#### Response Example
```json
{
   "object_name":"something.random"
}
```

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers.

**Code** : `403 Forbidden` When user has no access to bucket.

**Code** : `404 Not Found` When prefix not found.

**Code** : `400 Bad Request` In case of incorrect headers or multipart field values.

**Code** : `304 Not Modified` When modified_utc is less than or equal to existing object on the server.



## POST /riak/upload/[:bucket_id]/[:upload_id]/[:part_num]/ Upload big file

This API endpint should be used to continue upload of big file.


**Auth required** : YES

### Success Response

**Code** : `100 Continue`

**Code** : `200 OK`


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
FIRST_CHUNK_STATUS=`curl -s -X POST "http://127.0.0.1/riak/upload/the-poetry-naukovtsi-res/" \
    -F "files[]=@xaa;filename=something.random" \
    -F "object_name=Something.random" \
    -F "modified_utc=$TIMESTAMP" \
    -H "accept: application/json" \
    -H "content-range: bytes 0-$FIRST_CHUNK_RANGE_END/$FILE_SIZE_RANGE_TOTAL" \
    -H "authorization: Token $TOKEN"`

#
# Parse Upload ID
#
UPLOAD_ID=`echo $FIRST_CHUNK_STATUS|python -c "import sys, json; print json.load(sys.stdin)['upload_id']"`
OBJECT_NAME=`echo $FIRST_CHUNK_STATUS|python -c "import sys, json; print json.load(sys.stdin)['object_name']"`
FIRST_END_BYTE=`echo $FIRST_CHUNK_STATUS|python -c "import sys, json; print json.load(sys.stdin)['end_byte']"`
FIRST_MD5=`echo $FIRST_CHUNK_STATUS|python -c "import sys, json; print json.load(sys.stdin)['md5']"`
SECOND_MD5=`md5sum xab| awk '{ print $1 }'`

let "SECOND_START_BYTE=$FIRST_END_BYTE+1"

curl -s -X POST "http://127.0.0.1/riak/upload/the-poetry-naukovtsi-res/$UPLOAD_ID/2/" \
    -F 'files[]=@xab;filename=something.random' \
    -F "object_name=$OBJECT_NAME" \
    -F "modified_utc=$TIMESTAMP" \
    -F "etags[]=1,$FIRST_MD5,2,$SECOND_MD5" \
    -H "accept: application/json" \
    -H "content-range: bytes $SECOND_START_BYTE-$SECOND_CHUNK_RANGE_END/$FILE_SIZE_RANGE_TOTAL" \
    -H "authorization: Token $TOKEN"
```

#### Response Example
```json
{
   "upload_id":"qsf1Qzw4TgufqdNgNoByfQ",
   "object_name":"something.random",
   "end_byte":3631893,
   "md5":"89b10a5b5fd9e5ba1c97562ab67e54cc"
}
```

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` In case of incorrect headers or multipart field values



## POST /riak/copy/[:src_bucket_id]/ Copy object or directory

This API endpoint copies object using Riak CS COPY command.

**Auth required** : YES

### Success Response

**Code** : `204 No Content`

### Body

```json
{
   "src_object_names":["key 1", "key 2"],
   "dst_bucket_id":"string",
   "dst_prefix":"string",
   "src_prefix":"string"
}
```

#### Request Example
```sh
#
# The following request copies file "something.random" from pseudo-directory "demo" to "demo/test/".
#
curl -s -X POST "http://127.0.0.1/riak/copy/the-poetry-naukovtsi-res/" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -H "authorization: Token $TOKEN" \
    -d "{ \"src_prefix\": \"64656d6f/\", \"dst_prefix\": \"64656d6f/74657374/\", \"dst_bucket_id\": \"the-poetry-naukovtsi-res\", \"src_object_names\": [\"something.random\"] }"
```

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` In case of incorrect headers or multipart field values



## POST /riak/move/[:src_bucket_id]/ Move object or directory

This API endpoint copies object using Riak CS COPY command and then deletes it in previous location.
This might be suboptimal, but currently Riak CS do not have MOVE command.

**Auth required** : YES

### Success Response

**Code** : `204 No Content`

### Body

```json
{
   "src_object_names":["string 1", "string 2"],
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
    -d "{ \"src_prefix\": \"64656d6f/\", \"dst_prefix\": \"64656d6f/74657374/\", \"dst_bucket_id\": \"the-poetry-naukovtsi-res\", \"src_object_names\": [\"something.random\"] }"
```

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` In case of incorrect headers or multipart field values



## POST /riak/rename/[:src_bucket_id]/ Rename object or directory

Changes ``"orig_name"`` meta tag when called on object.
Moves nested objects to new prefix when used on pseudo-directories.

**Auth required** : YES

### Success Response

**Code** : `204 No Content`

### Body

```json
{
   "src_object_name":"string",
   "dst_object_name":"string",
   "prefix":"string"
}
```

`src_object_name` is an object **key**, but dst_object_name is UTF8 name.
Keys are always ASCII, but original names are stored in hex-encoded form.

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
    -d "{ \"prefix\": \"64656d6f/\", \"src_object_name\": \"something-random.jpg\", \"dst_object_name\": \"Something Something.jpg\" }"
```

### Other Response Codes

**Code** : `401 Unauthorized` When token is not provided in headers

**Code** : `403 Forbidden` When user has no access to bucket

**Code** : `404 Not Found` When prefix not found

**Code** : `400 Bad Request` In case of incorrect headers or multipart field values

