"""Run pytest and load testing"""
from ring_buffer.test.test_buffer import test_ring_is_full


def main():
    """Main function run when this file is run
    """
    test_ring_is_full()


if __name__ == '__main__':
    main()
