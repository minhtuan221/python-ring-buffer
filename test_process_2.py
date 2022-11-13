from ring_buffer.services.shared_list_object import SharedDictObject


shm = SharedDictObject('test')

print(shm.keys())