#!/bin/bash
# Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved. 
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License
# is located at
#
#     http://aws.amazon.com/apache2.0/ 
#
# or in the "LICENSE.txt" file accompanying this file. This file is 
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY 
# KIND, either express or implied. See the License for the specific language 
# governing permissions and limitations under the License.

nohup python3 worker.py my-first-stream --sleep_interval 0.1 --worker_time 345600 > 01worker.out 2> 01worker.err < /dev/null &
