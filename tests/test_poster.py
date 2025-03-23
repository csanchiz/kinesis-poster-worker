import unittest
from unittest.mock import patch, MagicMock, call
import json
import datetime
import sys
import os

# Add parent directory to path to import poster module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import poster

class TestPoster(unittest.TestCase):
    
    def setUp(self):
        self.mock_kinesis = MagicMock()
        self.stream_name = "test-stream"
        self.partition_key = "test-key"
        
    def test_make_string(self):
        """Test the make_string function generates strings of correct length"""
        length = 10
        result = poster.make_string(length)
        self.assertEqual(len(result), length)
        self.assertTrue(all(c.islower() for c in result))
        
    @patch('poster.kinesis')
    def test_get_or_create_stream_existing(self, mock_kinesis):
        """Test get_or_create_stream when stream exists"""
        mock_response = {
            'StreamDescription': {
                'StreamStatus': 'ACTIVE',
                'StreamName': self.stream_name
            }
        }
        mock_kinesis.describe_stream.return_value = mock_response
        
        result = poster.get_or_create_stream(mock_kinesis, self.stream_name, 1)
        
        mock_kinesis.describe_stream.assert_called_once_with(StreamName=self.stream_name)
        mock_kinesis.create_stream.assert_not_called()
        self.assertEqual(result, mock_response)
        
    @patch('poster.kinesis')
    @patch('time.sleep')
    def test_get_or_create_stream_not_existing(self, mock_sleep, mock_kinesis):
        """Test get_or_create_stream when stream doesn't exist"""
        # First call raises exception, second call returns inactive stream, third returns active
        mock_kinesis.describe_stream.side_effect = [
            mock_kinesis.exceptions.ResourceNotFoundException(),
            {'StreamDescription': {'StreamStatus': 'CREATING', 'StreamName': self.stream_name}},
            {'StreamDescription': {'StreamStatus': 'ACTIVE', 'StreamName': self.stream_name}}
        ]
        
        result = poster.get_or_create_stream(mock_kinesis, self.stream_name, 1)
        
        mock_kinesis.create_stream.assert_called_once_with(
            StreamName=self.stream_name, ShardCount=1)
        self.assertEqual(mock_kinesis.describe_stream.call_count, 3)
        self.assertEqual(result['StreamDescription']['StreamStatus'], 'ACTIVE')
        
    def test_sum_posts(self):
        """Test sum_posts function"""
        mock_poster1 = MagicMock()
        mock_poster1.total_records = 10
        mock_poster2 = MagicMock()
        mock_poster2.total_records = 15
        
        result = poster.sum_posts([mock_poster1, mock_poster2])
        self.assertEqual(result, 25)
        
    def test_kinesis_poster_init(self):
        """Test KinesisPoster initialization"""
        test_poster = poster.KinesisPoster(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            partition_key=self.partition_key
        )
        
        self.assertEqual(test_poster.stream_name, self.stream_name)
        self.assertEqual(test_poster.partition_key, self.partition_key)
        self.assertEqual(test_poster.total_records, 0)
        self.assertEqual(len(test_poster._pending_records), 0)
        
    def test_kinesis_poster_add_records(self):
        """Test KinesisPoster add_records method"""
        test_poster = poster.KinesisPoster(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            partition_key=self.partition_key
        )
        
        # Test adding a list of records
        records = ["record1", "record2"]
        test_poster.add_records(records)
        self.assertEqual(test_poster._pending_records, records)
        
        # Test adding a list containing a list of records
        test_poster._pending_records = []
        test_poster.add_records([["record3", "record4"]])
        self.assertEqual(test_poster._pending_records, ["record3", "record4"])
        
    def test_kinesis_poster_put_all_records(self):
        """Test KinesisPoster put_all_records method"""
        test_poster = poster.KinesisPoster(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            partition_key=self.partition_key
        )
        
        # Mock the put_records method
        test_poster.put_records = MagicMock()
        
        # Add records and call put_all_records
        test_poster._pending_records = ["record1", "record2"]
        result = test_poster.put_all_records()
        
        # Verify results
        test_poster.put_records.assert_called_once_with(["record1", "record2"])
        self.assertEqual(result, 2)
        self.assertEqual(test_poster.total_records, 2)
        self.assertEqual(test_poster._pending_records, [])
        
    def test_kinesis_poster_put_records(self):
        """Test KinesisPoster put_records method"""
        test_poster = poster.KinesisPoster(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            partition_key=self.partition_key,
            quiet=True
        )
        
        # Mock the kinesis client's put_record method
        self.mock_kinesis.put_record.return_value = {'SequenceNumber': '123'}
        
        # Call put_records
        records = ["record1", "record2"]
        test_poster.put_records(records)
        
        # Verify kinesis client was called correctly
        expected_calls = [
            call(StreamName=self.stream_name, Data="record1", PartitionKey=self.partition_key),
            call(StreamName=self.stream_name, Data="record2", PartitionKey=self.partition_key)
        ]
        self.mock_kinesis.put_record.assert_has_calls(expected_calls)
        
    def test_kinesis_poster_put_file_contents(self):
        """Test KinesisPoster put_file_contents method"""
        test_poster = poster.KinesisPoster(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            partition_key=self.partition_key,
            quiet=True
        )
        
        # Set file_contents and mock kinesis client
        test_poster.file_contents = "test file contents"
        self.mock_kinesis.put_record.return_value = {'SequenceNumber': '123'}
        
        # Call put_file_contents
        test_poster.put_file_contents()
        
        # Verify kinesis client was called correctly
        self.mock_kinesis.put_record.assert_called_once_with(
            StreamName=self.stream_name,
            Data="test file contents",
            PartitionKey=self.partition_key
        )
        self.assertEqual(test_poster.total_records, 1)
        
    @patch('datetime.datetime')
    def test_kinesis_poster_run(self, mock_datetime):
        """Test KinesisPoster run method"""
        test_poster = poster.KinesisPoster(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            partition_key=self.partition_key,
            poster_time=1,  # Short time for test
            quiet=True
        )
        
        # Mock datetime to control the loop
        now = datetime.datetime(2023, 1, 1, 12, 0, 0)
        later = datetime.datetime(2023, 1, 1, 12, 0, 2)
        mock_datetime.now.side_effect = [now, now, later]
        mock_datetime.timedelta.side_effect = lambda seconds: datetime.timedelta(seconds=seconds)
        
        # Mock methods
        test_poster.put_file_contents = MagicMock()
        test_poster.add_records = MagicMock()
        test_poster.put_all_records = MagicMock(return_value=5)
        
        # Run the method
        test_poster.run()
        
        # Verify the correct methods were called
        test_poster.add_records.assert_called_once()
        test_poster.put_all_records.assert_called_once()
        test_poster.put_file_contents.assert_not_called()  # No file contents

if __name__ == '__main__':
    unittest.main()
