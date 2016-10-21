import argparse
import os
from time import time


class FileManager:
    last_number = 0

    def __init__(self, device='.'):
        self.device = device
        self.block_size = os.stat(self.device).st_blksize
        self.tmp = os.path.join(self.device, 'tmp_zdb')
        self.prepare()

    def prepare(self) -> None:
        try:
            os.mkdir(self.tmp)
        except FileExistsError:
            self.clean()
            self.prepare()

    def clean(self) -> None:
        try:
            os.rmdir(self.tmp)
        except OSError:
            for root, dirs, files in os.walk(self.tmp, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(self.tmp)

    def make_file(self, blocks=1, remove=True) -> (str, float):
        """
        :param blocks: hwo many block file should have
        :param remove: if True, file is removed instantly after time measurement
        :return: str file name and time used to create file
        """
        self.last_number += 1
        file_path = os.path.join(self.tmp, str(self.last_number))
        data = bytes(self.block_size * blocks * ' ', encoding='ASCII')
        t = time()
        with open(file_path, 'wb', buffering=0) as f:
            f.write(data)
        t = time() - t
        if remove:
            os.remove(file_path)
        return file_path, t

    def read_file(self, file_path) -> int:
        t = time()
        with open(file_path, 'rb', buffering=0) as f:
            while f.read(self.block_size):
                pass
        return time() - t

    def __del__(self):
        self.clean()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--steps', type=int, help='How many steps to calculate avg', default=100)
    parser.add_argument('-b', '--blocks', type=int, help='How many blocks file should have', default=1)
    parser.add_argument('-d', '--device', type=str, help='Which device should be used', default='.')
    args = parser.parse_args()
    steps = args.steps
    file_block_size = args.blocks
    device = args.device

    total_write_time = 0
    total_read_time = 0
    total_read_one_file_time = 0

    fm = FileManager(device)

    # many files write and read
    for _ in range(steps):
        file_name, partial_time = fm.make_file(file_block_size, False)
        total_write_time += partial_time
        total_read_time = fm.read_file(file_name)
        os.remove(file_name)
    print('Write average time:', total_write_time / steps)
    print('Read file after save average time:', total_read_time / steps)

    # single file read
    file_to_read = fm.make_file(5, False)[0]
    for _ in range(steps):
        total_read_one_file_time += fm.read_file(file_to_read)
    print('Read one file average time:', total_read_one_file_time / steps)
