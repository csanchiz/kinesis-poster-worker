import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import json
import boto3
from botocore.stub import Stubber

# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import poster
import worker

class TestIntegration(unittest.TestCase):
    """Integration tests for poster and worker interaction"""
    
    def setUp(self):
        self.stream_name = "test-integration-stream"
        self.region = "us-east-1"
        self.shard_count = 1
        
        # Create stubbed kinesis client
        self.kinesis_client = boto3.client('kinesis', region_name=self.region)
        self.stubber = Stubber(self.kinesis_client)
        
    def test_poster_worker_integration(self):
        """Test that records posted by KinesisPoster can be read by KinesisWorker"""
        # Setup stubs for poster operations
        
        # 1. Describe stream response for poster
        describe_stream_response = {
            'StreamDescription': {
                'StreamName': self.stream_name,
                'StreamARN': f'arn:aws:kinesis:{self.region}:123456789012:stream/{self.stream_name}',
                'StreamStatus': 'ACTIVE',
                'Shards': [
                    {
                        'ShardId': 'shardId-000000000000',
                        'HashKeyRange': {
                            'StartingHashKey': '0',
                            'EndingHashKey': '340282366920938463463374607431768211455'
                        },
                        'SequenceNumberRange': {
                            'StartingSequenceNumber': '49613747809830099601876892869489827638947288984959557634'
                        }
                    }
                ],
                'HasMoreShards': False,
                'RetentionPeriodHours': 24,
                'StreamCreationTimestamp': 1617979537.0,
                'EnhancedMonitoring': [{'ShardLevelMetrics': []}],
                'EncryptionType': 'NONE'
            }
        }
        
        self.stubber.add_response('describe_stream', describe_stream_response, {'StreamName': self.stream_name})
        
        # 2. Put record responses for poster
        put_record_response = {
            'ShardId': 'shardId-000000000000',
            'SequenceNumber': '49613747809830099601876892869489827638947288984959557634'
        }
        
        # We'll put 3 records
        for _ in range(3):
            self.stubber.add_response('put_record', put_record_response, {
                'StreamName': self.stream_name,
                'Data': unittest.mock.ANY,
                'PartitionKey': unittest.mock.ANY
            })
        
        # 3. Describe stream response for worker (same as poster)
        self.stubber.add_response('describe_stream', describe_stream_response, {'StreamName': self.stream_name})
        
        # 4. Get shard iterator response for worker
        get_shard_iterator_response = {
            'ShardIterator': 'AAAAAAAAAAHSywljv0zEgPX4NyKdZ5wryMzP9yALs8NeKbUjp1IxtZs1Sp+KEd9I6AJ9ZG4lNR1EMi+9Md/nHvtLyxpfhEzYvkTZ4D9DQVz/mBYWRO6OTZRKnW9gd+efGN2aHFdkH1rJl4BL9Wyrk+ghYG22D2T1Da2EyNSH1+LAbK33gQweTJADBdyMwlo5r6PqcP2dzhg='
        }
        
        self.stubber.add_response('get_shard_iterator', get_shard_iterator_response, {
            'StreamName': self.stream_name,
            'ShardId': 'shardId-000000000000',
            'ShardIteratorType': 'LATEST'
        })
        
        # 5. Get records response for worker
        get_records_response = {
            'Records': [
                {
                    'SequenceNumber': '49613747809830099601876892869489827638947288984959557634',
                    'ApproximateArrivalTimestamp': 1617979537.0,
                    'Data': b'test data with egg inside',
                    'PartitionKey': 'test-key'
                },
                {
                    'SequenceNumber': '49613747809830099601876892869489827638947288984959557635',
                    'ApproximateArrivalTimestamp': 1617979538.0,
                    'Data': b'another record with no special words',
                    'PartitionKey': 'test-key'
                },
                {
                    'SequenceNumber': '49613747809830099601876892869489827638947288984959557636',
                    'ApproximateArrivalTimestamp': 1617979539.0,
                    'Data': b'final record with eggs',
                    'PartitionKey': 'test-key'
                }
            ],
            'NextShardIterator': 'AAAAAAAAAAGb7RPGl1/XDf+QEN0Xw93iHQiFXQM0KmO34Z6WQQpxs5Uw/rZEMGlV1SsTvB+nzTwzmdkX292zmqfgKWpVnISj5ZkU7hXyJso+Te7dRfwUOYvxo3b4wESpKdL1UyI3XD8e7rv9cSoWr1Q9/B3bv6tqzYgzn+yYIzSnwqQqRp1jRWGsRKJ8onyFRanT8KU=',
            'MillisBehindLatest': 0
        }
        
        self.stubber.add_response('get_records', get_records_response, {
            'ShardIterator': get_shard_iterator_response['ShardIterator'],
            'Limit': 25
        })
        
        # Start using the stubber
        self.stubber.activate()
        
        try:
            # Create a poster and post some records
            test_poster = poster.KinesisPoster(
                kinesis_client=self.kinesis_client,
                stream_name=self.stream_name,
                partition_key="test-key",
                poster_time=1,
                quiet=True
            )
            
            # Add some test records
            test_records = ["test data with egg inside", "another record with no special words", "final record with eggs"]
            test_poster.add_records(test_records)
            test_poster.put_all_records()
            
            # Create a worker to process the records
            with patch('sys.stdout'):  # Suppress output for testing
                test_worker = worker.KinesisWorker(
                    kinesis_client=self.kinesis_client,
                    stream_name=self.stream_name,
                    shard_id="shardId-000000000000",
                    iterator_type="LATEST",
                    worker_time=1,
                    sleep_interval=0.1
                )
                
                # Process records
                with patch('worker.find_eggs') as mock_find_eggs:
                    test_worker.run()
                    
                    # Verify find_eggs was called with the records
                    mock_find_eggs.assert_called_once()
                    
                    # Get the records that were passed to find_eggs
                    actual_records = mock_find_eggs.call_args[0][0]
                    
                    # Verify we got the expected number of records
                    self.assertEqual(len(actual_records), 3)
                    
                    # Verify the record content
                    self.assertEqual(actual_records[0]['Data'], b'test data with egg inside')
                    self.assertEqual(actual_records[1]['Data'], b'another record with no special words')
                    self.assertEqual(actual_records[2]['Data'], b'final record with eggs')
        
        finally:
            self.stubber.deactivate()
            
    def test_poster_worker_echo_integration(self):
        """Test that records posted by KinesisPoster can be echoed by KinesisWorker"""
        # Setup stubs similar to previous test
        describe_stream_response = {
            'StreamDescription': {
                'StreamName': self.stream_name,
                'StreamARN': f'arn:aws:kinesis:{self.region}:123456789012:stream/{self.stream_name}',
                'StreamStatus': 'ACTIVE',
                'Shards': [
                    {
                        'ShardId': 'shardId-000000000000',
                        'HashKeyRange': {
                            'StartingHashKey': '0',
                            'EndingHashKey': '340282366920938463463374607431768211455'
                        },
                        'SequenceNumberRange': {
                            'StartingSequenceNumber': '49613747809830099601876892869489827638947288984959557634'
                        }
                    }
                ],
                'HasMoreShards': False,
                'RetentionPeriodHours': 24,
                'StreamCreationTimestamp': 1617979537.0,
                'EnhancedMonitoring': [{'ShardLevelMetrics': []}],
                'EncryptionType': 'NONE'
            }
        }
        
        self.stubber.add_response('describe_stream', describe_stream_response, {'StreamName': self.stream_name})
        
        # Put record responses
        put_record_response = {
            'ShardId': 'shardId-000000000000',
            'SequenceNumber': '49613747809830099601876892869489827638947288984959557634'
        }
        
        self.stubber.add_response('put_record', put_record_response, {
            'StreamName': self.stream_name,
            'Data': unittest.mock.ANY,
            'PartitionKey': unittest.mock.ANY
        })
        
        # Worker responses
        self.stubber.add_response('describe_stream', describe_stream_response, {'StreamName': self.stream_name})
        
        get_shard_iterator_response = {
            'ShardIterator': 'AAAAAAAAAAHSywljv0zEgPX4NyKdZ5wryMzP9yALs8NeKbUjp1IxtZs1Sp+KEd9I6AJ9ZG4lNR1EMi+9Md/nHvtLyxpfhEzYvkTZ4D9DQVz/mBYWRO6OTZRKnW9gd+efGN2aHFdkH1rJl4BL9Wyrk+ghYG22D2T1Da2EyNSH1+LAbK33gQweTJADBdyMwlo5r6PqcP2dzhg='
        }
        
        self.stubber.add_response('get_shard_iterator', get_shard_iterator_response, {
            'StreamName': self.stream_name,
            'ShardId': 'shardId-000000000000',
            'ShardIteratorType': 'LATEST'
        })
        
        get_records_response = {
            'Records': [
                {
                    'SequenceNumber': '49613747809830099601876892869489827638947288984959557634',
                    'ApproximateArrivalTimestamp': 1617979537.0,
                    'Data': b'test data for echo',
                    'PartitionKey': 'test-key'
                }
            ],
            'NextShardIterator': 'AAAAAAAAAAGb7RPGl1/XDf+QEN0Xw93iHQiFXQM0KmO34Z6WQQpxs5Uw/rZEMGlV1SsTvB+nzTwzmdkX292zmqfgKWpVnISj5ZkU7hXyJso+Te7dRfwUOYvxo3b4wESpKdL1UyI3XD8e7rv9cSoWr1Q9/B3bv6tqzYgzn+yYIzSnwqQqRp1jRWGsRKJ8onyFRanT8KU=',
            'MillisBehindLatest': 0
        }
        
        self.stubber.add_response('get_records', get_records_response, {
            'ShardIterator': get_shard_iterator_response['ShardIterator'],
            'Limit': 25
        })
        
        # Start using the stubber
        self.stubber.activate()
        
        try:
            # Create a poster and post a record
            test_poster = poster.KinesisPoster(
                kinesis_client=self.kinesis_client,
                stream_name=self.stream_name,
                partition_key="test-key",
                poster_time=1,
                quiet=True
            )
            
            test_poster.put_records(["test data for echo"])
            
            # Create a worker with echo=True
            with patch('sys.stdout'):
                with patch('worker.echo_records') as mock_echo:
                    test_worker = worker.KinesisWorker(
                        kinesis_client=self.kinesis_client,
                        stream_name=self.stream_name,
                        shard_id="shardId-000000000000",
                        iterator_type="LATEST",
                        worker_time=1,
                        sleep_interval=0.1,
                        echo=True
                    )
                    
                    test_worker.run()
                    
                    # Verify echo_records was called with the record
                    mock_echo.assert_called_once()
                    actual_records = mock_echo.call_args[0][0]
                    self.assertEqual(len(actual_records), 1)
                    self.assertEqual(actual_records[0]['Data'], b'test data for echo')
        
        finally:
            self.stubber.deactivate()

if __name__ == '__main__':
    unittest.main()
