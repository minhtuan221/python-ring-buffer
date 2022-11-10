from multiprocessing import shared_memory
import typing as t
import pickle
import time


class SharedListObject:
    def __init__(self,
                 name: str,
                 size: int,
                 element_size=255,
                 create=True) -> None:
        self._element_size = element_size
        self.name = name
        # size of the element in ShareableList = math.ceil((element_size+1)/8)*8)
        if create:
            self.share_list = \
                shared_memory.ShareableList([bytes(self._element_size) for _ in range(size)],
                                            name=self.name)
        else:
            self.share_list = \
                shared_memory.ShareableList(name=self.name)

    def set(self, _index: int, obj: t.Any):
        obj_bytes = pickle.dumps(obj)
        self.share_list[_index] = obj_bytes

    def get(self, _index: int):
        obj_bytes = self.share_list[_index]
        if len(obj_bytes) == 0:
            return None
        return pickle.loads(obj_bytes)

    def remove(self, _index: int = -1) -> t.Any:
        obj_bytes = self.share_list[_index]
        self.share_list[_index] = bytes(self._element_size)
        if len(obj_bytes) == 0:
            return None
        return pickle.loads(obj_bytes)

    def shutdown(self):
        self.share_list.shm.unlink()



def maintain_share_list_object():
    smm = SharedListObject('test1', 255)
    smm.set(0, 'first element')
    while True:
        time.sleep(10)
    smm.shutdown()


def main():
    smm = SharedListObject('test', 16)
    _next = ''
    for i in range(20):
        _next += f'{i}'
        smm.set(1, bytes(_next, encoding='utf8'))
        v = smm.get(i)
        print(v, type(v), len(v))
        # print(len(smm.share_list))
    print(smm.share_list)


if __name__ == "__main__":
    maintain_share_list_object()
