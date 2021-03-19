import argparse
from contextlib import closing
from fnmatch import fnmatch
from os import listdir, remove, rename
from os.path import join, exists
import shutil

CHECKSUMS_FILE_NAME = 'checksums.csv'
WEBIN_MANIFEST_FILE_NAME = ''


class UploadUtils:
    def __init__(self, secure_key, webin_user, s3_root='/mnt/s3Load/', webin_root='/mnt/webin/'):
        self.s3_folder = self.setup_s3(s3_root, secure_key)
        self.webin_folder = self.setup_webin(webin_root, webin_user)
        self.checksums_path = join(self.s3_folder, CHECKSUMS_FILE_NAME)
        self.checksum_map = self.load_checksums_file(self.checksums_path)
        self.save_checksums = False
    
    def remove_checksum_from_files(self):
        for s3_file in listdir(self.s3_folder):
            if self.ends_with_checksum(s3_file):
                name_without_checksum = self.get_name_without_checksum(s3_file)
                print('New S3 file detected {}'.format(name_without_checksum))
                self.rename_file(self.s3_folder, s3_file, name_without_checksum)
                self.remove_stale_file(join(self.webin_folder, name_without_checksum))

    def validate_files(self):
        pass

    def copy_files_to_webin(self):
        for s3_file in listdir(self.s3_folder):
            if s3_file in self.checksum_map:  # File previously had a checksum which has been removed
                s3_file_path = join(self.s3_folder, s3_file)
                webin_file_path = join(self.webin_folder, s3_file)
                if exists(webin_file_path):
                    print('File already exists {}'.format(webin_file_path))
                else:
                    print('Copying {} to {}'.format(s3_file_path, webin_file_path))
                    shutil.copyfile(s3_file_path, webin_file_path)

    def close(self):
        if self.save_checksums:
            self.save_checksums_file(self.checksums_path, self.checksum_map)

    def get_name_without_checksum(self, name_with_checksum):
        name_without_checksum, _, checksum = name_with_checksum.rpartition('.')
        self.checksum_map[name_without_checksum] = checksum
        self.save_checksums = True
        return name_without_checksum

    @staticmethod
    def ends_with_checksum(file_name):
        return len(file_name.rpartition('.')[2]) == 32

    @staticmethod
    def rename_file(folder, old_name, new_name):
        path_with_checksum = join(folder, old_name)
        path_without_checksum = join(folder, new_name)
        UploadUtils.remove_stale_file(path_without_checksum)
        rename(path_with_checksum, path_without_checksum)

    @staticmethod
    def remove_stale_file(file_path):
        if exists(file_path):
            print('Removing stale file {}'.format(file_path))
            remove(file_path)

    @staticmethod
    def setup_s3(s3_root, secure_key):
        s3_folder = join(s3_root, secure_key)
        if not exists(s3_folder):
            raise EnvironmentError("s3 Folder {} doesn't exist.".format(s3_folder))
        return s3_folder

    @staticmethod
    def setup_webin(webin_root, webin_user):
        webin_number = webin_user.rpartition('-')[2]
        webin_folder = join(webin_root, webin_number)
        # ToDo: Could setup webin mount here if it doesn't exist
        if not exists(webin_folder):
            raise NotImplementedError("Webin Folder {} doesn't exist and the tool can't create it (yet).".format(webin_folder))
        return webin_folder

    @staticmethod
    def load_checksums_file(checksums_path):
        checksum_map = {}
        if not exists(checksums_path):
            return checksum_map
        with open(checksums_path, "r") as checksums_file:
            line = checksums_file.readline()
            while line:
                file_name, _, file_checksum = line.strip().partition(',')
                checksum_map[file_name] = file_checksum
                line = checksums_file.readline()
        return checksum_map
    
    @staticmethod
    def save_checksums_file(checksums_path, checksum_map):
        file_checksums = []
        for file_name, checksum in checksum_map.items(): 
            file_checksums.append('{},{}'.format(file_name, checksum))
        if exists(checksums_path):
            remove(checksums_path)
        print('Saving checksums file: {}'.format(checksums_path))
        with open(checksums_path, "w") as checksums_file:
            checksums_file.write("\n".join(file_checksums))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Prepare s3 drag-and-drop files and transfer to Webin'
    )
    parser.add_argument(
        'secure_key', type=str,
        help='The secure key used when uploading files to the drag-and-drop data submission tool. Format: xxxxx-xxx-xxxx-xxxxx'
    )
    parser.add_argument(
        'webin_user', type=str,
        help='The Webin User Account used when uploading files to webin: Format: Webin-58468'
    )
    parser.add_argument(
        '--s3_root', type=str, default='/mnt/s3Load/', help='Absolute path of mounted s3 folder'
    )
    parser.add_argument(
        '--webin_root', type=str, default='/mnt/webin/', help='Absolute path of mounted webin folder'
    )
    args = parser.parse_args()
    with closing(UploadUtils(args.secure_key, args.webin_user, s3_root=args.s3_root, webin_root=args.webin_root)) as upload_utils:
        upload_utils.remove_checksum_from_files()
        upload_utils.validate_files()
        upload_utils.copy_files_to_webin()
