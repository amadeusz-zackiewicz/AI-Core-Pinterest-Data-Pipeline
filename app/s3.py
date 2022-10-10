import boto3
import re
import io
import json
import os

class S3Client():
    def __init__(self, name_pattern: str, region: str):
        """
        Connect to a S3 bucket.

        Args:
            bucket_name: the name of the bucket
            region: a region in which the bucket is located
        """
        self.client = boto3.client("s3")
        self.bucket_region = region
        self.bucket = None

        buckets = self.client.list_buckets()

        for bucket_name in [b["Name"] for b in buckets["Buckets"]]:
            if re.search(name_pattern, bucket_name) != None:
                self.bucket = bucket_name
                print("Bucket selected:", self.bucket)
                break

        if self.bucket == None:
            print("Buckets found:")
            for bn in [b["Name"] for b in buckets["Buckets"]]:
                print(bn)
            raise Exception(f"No bucket that matches '{name_pattern}' has been found")



    def upload_obj(self, file_name: str, data):
        """
        Upload specified data stream to the bucket.

        Args:
            file_name: the name under which the file will be stored, including the folder name
            data: the data stream to send
        """
        f = io.BytesIO(json.dumps(data).encode("ascii"))
        result = self.client.upload_fileobj(Fileobj=f, Bucket=self.bucket, Key=file_name)
        f.close()

    @staticmethod
    def __print_bytes_sent(count):
        print("Bytes sent:", count)

    def upload_data(self, file_name: str, data):
        """
        Upload specified data stream to the bucket.

        Args:
            file_name: the name under which the file will be stored, including the folder name
            data: the data stream to send
        """
        self.upload_obj(f"{file_name}.json", data)

    def upload_image(self, file_name: str, data):
        """
        Upload specified image stream to the bucket.

        Args:
            file_name: the name under which the file will be stored, including the folder name
            data: the image stream to send
        """
        self.upload_obj(f"{file_name}.jpeg", data)

    def file_exists(self, file_name: str) -> bool:
        """
        Check if the specified file already exists in the bucket

        Args:
            file_name: the name of the file, including folder name

        Returns:
            bool
        """
        try:
            l = self.client.head_object(Bucket=self.bucket, Key="file_path")
            return True
        except:
            return False

    def get_file_list(self):
        objects = self.client.list_objects_v2(Bucket=self.bucket)
        return [o["Key"] for o in objects["Contents"]]

    def get_all_files(self, where = None, delete_old = True):
        objects = self.get_file_list()
        file_count = len(objects)
        if where == None:
            mem_objs = []
            for i, o in enumerate(objects):
                f = io.BytesIO()
                self.client.download_fileobj(self.bucket, o, f)
                mem_objs.append(f)
                print(f"Download: {i}/{file_count}", end="\r")
            print("Download finished")
            return mem_objs
        else:
            all_paths = []
            if delete_old:
                for f in os.listdir(where):
                    if os.path.isfile(f"{where}/{f}"):
                        os.remove(f"{where}/{f}")
            for i, o in enumerate(objects):
                file_path = f"{where}/{o}"
                f = open(file_path, "wb")
                self.client.download_file(self.bucket, o, file_path)
                f.close()
                all_paths.append(file_path)
                print(f"Download: {i}/{file_count}", end="\r")
            print("Download finished")

    def close(self):
        if self.tmp_bucket:
            self.client.delete_bucket()
        self.client.close()

    def __create_bucket(self):
        response = self.client.create_bucket(
            ACL="public-read",
            Bucket=self.bucket, 
            CreateBucketConfiguration=
            {
                "LocationConstraint": self.bucket_region
            },
            ObjectLockEnabledForBucket=False,
            ObjectOwnership="BucketOwnerPreferred"
            )