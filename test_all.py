"""Run pytest and load testing"""
from ring_buffer.test.test_buffer import test_ring_is_full
from ring_buffer.test.test_shared_list_1 import test_get_set
from ring_buffer.services.shared_list_object import test_dynamic_dict_obj, test_share_list_object_1


def main():
    """Main function run when this file is run
    """
    # test_ring_is_full()
    # test_get_set()
    # test_dynamic_dict_obj()
    test_share_list_object_1()


if __name__ == '__main__':
    main()
