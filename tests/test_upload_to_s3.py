import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from botocore.exceptions import ClientError

from src.ingestion.upload_to_s3 import upload_batch


class TestUploadBatch(unittest.TestCase):

    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.file_path = (
            Path(self.temp_dir.name) / "batch_20260102.csv"
        )
        self.file_path.write_text("id,shares\n1,100\n")

        self.s3 = Mock()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_existing_object_is_skipped(self):
        self.s3.head_object.return_value = {
            "ContentLength": 100
        }

        result = upload_batch(
            self.s3,
            self.file_path,
            "20260102"
        )

        self.assertEqual(result, "SKIPPED")
        self.s3.upload_file.assert_not_called()

    def test_missing_object_is_uploaded(self):
        self.s3.head_object.side_effect = ClientError(
            {
                "Error": {
                    "Code": "404",
                    "Message": "Not Found"
                }
            },
            "HeadObject"
        )

        result = upload_batch(
            self.s3,
            self.file_path,
            "20260102"
        )

        self.assertEqual(result, "UPLOADED")
        self.s3.put_object.assert_called_once()

    def test_access_denied_is_not_ignored(self):
        self.s3.head_object.side_effect = ClientError(
            {
                "Error": {
                    "Code": "403",
                    "Message": "Access Denied"
                }
            },
            "HeadObject"
        )

        with self.assertRaises(ClientError):
            upload_batch(
                self.s3,
                self.file_path,
                "20260102"
            )

        self.s3.upload_file.assert_not_called()

    def test_missing_local_file(self):
        missing_file = (
            Path(self.temp_dir.name) / "missing.csv"
        )

        with self.assertRaises(FileNotFoundError):
            upload_batch(
                self.s3,
                missing_file,
                "20260102"
            )

        self.s3.upload_file.assert_not_called()


if __name__ == "__main__":
    unittest.main()