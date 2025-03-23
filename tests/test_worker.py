import unittest
from unittest.mock import patch, MagicMock, call
import json
import datetime
import sys
import os
import io

# Add parent directory to path to import worker module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import worker

class TestWorker(unittest.TestCase):
    
    def setUp(self):
        self.mock_kinesis = MagicMock()
        self.stream_name = "test-stream"
        self.shard_id = "shardId-000000000000"
        
    @patch('sys.stdout', new_callable=io.StringIO)
    def test_find_eggs_with_match(self, mock_stdout):
        """Test find_eggs function when 'egg' is found in records"""
        records = [
            {'Data': b'This contains an egg somewhere'},
            {'Data': b'No matches here'},
            {'Data': b'Multiple eggs and more eggs'}
        ]
        
        worker.find_eggs(records)
        
        output = mock_stdout.getvalue()
        self.assertIn('egg location:', output)
        self.assertIn('[15]', output)  # Position of 'egg' in first record
        self.assertIn('[9, 23]', output)  # Positions of 'egg' in third record
        
    @patch('sys.stdout', new_callable=io.StringIO)
    def test_find_eggs_no_match(self, mock_stdout):
        """Test find_eggs function when no 'egg' is found"""
        records = [
            {'Data': b'No matches here'},
            {'Data': b'Still nothing to find'}
        ]
        
        worker.find_eggs(records)
        
        output = mock_stdout.getvalue()
        self.assertEqual('', output)  # No output when no eggs found
        
    @patch('sys.stdout', new_callable=io.StringIO)
    def test_echo_records(self, mock_stdout):
        """Test echo_records function"""
        records = [
            {'Data': b'Record 1 content'},
            {'Data': b'Record 2 content'}
        ]
        
        worker.echo_records(records)
        
        output = mock_stdout.getvalue()
        self.assertIn('Record 1 content', output)
        self.assertIn('Record 2 content', output)
        
    def test_kinesis_worker_init(self):
        """Test KinesisWorker initialization"""
        test_worker = worker.KinesisWorker(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            shard_id=self.shard_id,
            iterator_type='LATEST'
        )
        
        self.assertEqual(test_worker.stream_name, self.stream_name)
        self.assertEqual(test_worker.shard_id, self.shard_id)
        self.assertEqual(test_worker.iterator_type, 'LATEST')
        self.assertEqual(test_worker.total_records, 0)
        
    @patch('threading.current_thread')
    @patch('worker.find_eggs')
    @patch('sys.stdout', new_callable=io.StringIO)
    @patch('datetime.datetime')
    @patch('time.sleep')
    def test_kinesis_worker_run(self, mock_sleep, mock_datetime, mock_stdout, mock_find_eggs, mock_thread):
        """Test KinesisWorker run method"""
        # Setup mocks
        mock_thread.return_value.name = "test-worker"
        
        now = datetime.datetime(2023, 1, 1, 12, 0, 0)
        later = datetime.datetime(2023, 1, 1, 12, 0, 2)
        mock_datetime.now.side_effect = [now, now, later]
        mock_datetime.timedelta.side_effect = lambda seconds: datetime.timedelta(seconds=seconds)
        
        # Mock kinesis client responses
        self.mock_kinesis.get_shard_iterator.return_value = {
            'ShardIterator': 'test-iterator'
        }
        
        self.mock_kinesis.get_records.return_value = {
            'Records': [
                {'Data': b'Record with egg'},
                {'Data': b'Another record'}
            ],
            'NextShardIterator': 'next-iterator'
        }
        
        # Create worker and run
        test_worker = worker.KinesisWorker(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            shard_id=self.shard_id,
            iterator_type='LATEST',
            worker_time=1,  # Short time for test
            sleep_interval=0.1
        )
        
        test_worker.run()
        
        # Verify correct methods were called
        self.mock_kinesis.get_shard_iterator.assert_called_once_with(
            StreamName=self.stream_name,
            ShardId=self.shard_id,
            ShardIteratorType='LATEST'
        )
        
        self.mock_kinesis.get_records.assert_called_once_with(
            ShardIterator='test-iterator',
            Limit=25
        )
        
        mock_find_eggs.assert_called_once()
        self.assertEqual(test_worker.total_records, 2)
        
    @patch('threading.current_thread')
    @patch('sys.stdout', new_callable=io.StringIO)
    @patch('datetime.datetime')
    @patch('time.sleep')
    def test_kinesis_worker_run_with_echo(self, mock_sleep, mock_datetime, mock_stdout, mock_thread):
        """Test KinesisWorker run method with echo=True"""
        # Setup mocks
        mock_thread.return_value.name = "test-worker"
        
        now = datetime.datetime(2023, 1, 1, 12, 0, 0)
        later = datetime.datetime(2023, 1, 1, 12, 0, 2)
        mock_datetime.now.side_effect = [now, now, later]
        mock_datetime.timedelta.side_effect = lambda seconds: datetime.timedelta(seconds=seconds)
        
        # Mock kinesis client responses
        self.mock_kinesis.get_shard_iterator.return_value = {
            'ShardIterator': 'test-iterator'
        }
        
        self.mock_kinesis.get_records.return_value = {
            'Records': [
                {'Data': b'Record with egg'},
                {'Data': b'Another record'}
            ],
            'NextShardIterator': 'next-iterator'
        }
        
        # Create worker and run with echo=True
        test_worker = worker.KinesisWorker(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            shard_id=self.shard_id,
            iterator_type='LATEST',
            worker_time=1,  # Short time for test
            sleep_interval=0.1,
            echo=True
        )
        
        test_worker.run()
        
        # Verify output contains echoed records
        output = mock_stdout.getvalue()
        self.assertIn('Record with egg', output)
        self.assertIn('Another record', output)
        
    @patch('threading.current_thread')
    @patch('sys.stdout', new_callable=io.StringIO)
    @patch('datetime.datetime')
    @patch('time.sleep')
    def test_kinesis_worker_run_no_records(self, mock_sleep, mock_datetime, mock_stdout, mock_thread):
        """Test KinesisWorker run method when no records are returned"""
        # Setup mocks
        mock_thread.return_value.name = "test-worker"
        
        now = datetime.datetime(2023, 1, 1, 12, 0, 0)
        later = datetime.datetime(2023, 1, 1, 12, 0, 2)
        mock_datetime.now.side_effect = [now, now, later]
        mock_datetime.timedelta.side_effect = lambda seconds: datetime.timedelta(seconds=seconds)
        
        # Mock kinesis client responses
        self.mock_kinesis.get_shard_iterator.return_value = {
            'ShardIterator': 'test-iterator'
        }
        
        self.mock_kinesis.get_records.return_value = {
            'Records': [],
            'NextShardIterator': 'next-iterator'
        }
        
        # Create worker and run
        test_worker = worker.KinesisWorker(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            shard_id=self.shard_id,
            iterator_type='LATEST',
            worker_time=1,  # Short time for test
            sleep_interval=0.1
        )
        
        test_worker.run()
        
        # Verify output contains a dot for no records
        output = mock_stdout.getvalue()
        self.assertIn('.', output)
        self.assertEqual(test_worker.total_records, 0)
        
    @patch('threading.current_thread')
    @patch('sys.stdout', new_callable=io.StringIO)
    @patch('datetime.datetime')
    @patch('time.sleep')
    def test_kinesis_worker_run_with_throughput_exception(self, mock_sleep, mock_datetime, mock_stdout, mock_thread):
        """Test KinesisWorker run method when ProvisionedThroughputExceededException occurs"""
        # Setup mocks
        mock_thread.return_value.name = "test-worker"
        
        now = datetime.datetime(2023, 1, 1, 12, 0, 0)
        later = datetime.datetime(2023, 1, 1, 12, 0, 2)
        mock_datetime.now.side_effect = [now, now, later]
        mock_datetime.timedelta.side_effect = lambda seconds: datetime.timedelta(seconds=seconds)
        
        # Mock kinesis client responses
        self.mock_kinesis.get_shard_iterator.return_value = {
            'ShardIterator': 'test-iterator'
        }
        
        # Create a mock exception
        throughput_exception = self.mock_kinesis.exceptions.ProvisionedThroughputExceededException(
            error_response={'Error': {'Code': 'ProvisionedThroughputExceededException'}},
            operation_name='GetRecords'
        )
        
        self.mock_kinesis.get_records.side_effect = throughput_exception
        
        # Create worker and run
        test_worker = worker.KinesisWorker(
            kinesis_client=self.mock_kinesis,
            stream_name=self.stream_name,
            shard_id=self.shard_id,
            iterator_type='LATEST',
            worker_time=1,  # Short time for test
            sleep_interval=0.1
        )
        
        test_worker.run()
        
        # Verify sleep was called with longer duration for backoff
        mock_sleep.assert_any_call(5)

if __name__ == '__main__':
    unittest.main()
