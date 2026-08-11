import io
from minio import Minio

# json_bytes = 

# def reading():
#     objects = list(minio_client.list_objects({bucket} , recursive = True))
#     if not objects:
#         print("can not find any file in raw bucket!")
#         return 
    
#     latest_objects = max(objects, key=lambda x: x.last_modified)
#     print(f" reading raw file: {latest_objects}")
    
#     response = minio_client.get_object({bucket}, latest_object.object_name)
#     json_bytes = response.read()
#     response.close()
#     response.release_conn()