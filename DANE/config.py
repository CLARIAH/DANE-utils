# Copyright 2020-present, Netherlands Institute for Sound and Vision (Nanne van Noord)
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##############################################################################

from yacs.config import CfgNode as CN
import os, sys, inspect
import inspect
import DANE.errors as errors

__all__ = ["cfg"]

cfg = CN(new_allowed=True)

cfg.DANE = CN(new_allowed=True)
cfg.DANE.HOST = '0.0.0.0' # host we listen on
cfg.DANE.PORT = 5500
if 'DANE_HOME' in os.environ.keys():
    cfg.DANE.HOME_DIR = os.path.join(os.environ['DANE_HOME'], '')
else:
    cfg.DANE.HOME_DIR = os.path.join(os.environ['HOME'], ".DANE", '')

# cwd might not be same as dir where the file being called is in, resolve this
# xdir, _ = os.path.split(os.path.join(os.getcwd(), sys.argv[0]))

cfg.DANE.API_URL = 'http://localhost:5500/DANE/' # URL the api can be reached at
cfg.DANE.MANAGE_URL = 'http://localhost:5500/manage/'

cfg.RABBITMQ = CN()
cfg.RABBITMQ.HOST = 'localhost'
cfg.RABBITMQ.PORT = 5672
cfg.RABBITMQ.EXCHANGE = 'DANE-exchange'
cfg.RABBITMQ.RESPONSE_QUEUE = 'DANE-response-queue'
cfg.RABBITMQ.USER = 'guest'
cfg.RABBITMQ.PASSWORD = 'guest'
cfg.RABBITMQ.MANAGEMENT = True # Set to false if no Rabbitmq management plugin
cfg.RABBITMQ.MANAGEMENT_PORT = 15672
cfg.RABBITMQ.MANAGEMENT_HOST = 'localhost'

cfg.ELASTICSEARCH = CN()
cfg.ELASTICSEARCH.HOST = ['localhost']
cfg.ELASTICSEARCH.PORT = 9200
cfg.ELASTICSEARCH.USER = 'elastic'
cfg.ELASTICSEARCH.PASSWORD = 'changeme'
cfg.ELASTICSEARCH.SCHEME = 'http'
cfg.ELASTICSEARCH.SHARDS = 1
cfg.ELASTICSEARCH.REPLICAS = 1
cfg.ELASTICSEARCH.TIMEOUT = 30 # in seconds
cfg.ELASTICSEARCH.MAX_RETRIES = 3 # 0 to disable retrying
cfg.ELASTICSEARCH.INDEX = 'dane-index'

cfg.CUDA = CN(new_allowed=True)
cfg.CUDA.VISIBLE_DEVICES = '1'

cfg.LOGGING = CN(new_allowed=True)
cfg.LOGGING.LEVEL = 'DEBUG'
cfg.LOGGING.DIR = '/var/logs/DANE'

cfg.CONFIG = CN(new_allowed=False)
# If a base.config.yml sets this to true, then a config.yml is required
cfg.CONFIG.REQUIRED = False

cfg.PATHS = CN(new_allowed=True)
cfg.PATHS.TEMP_FOLDER = './TEMP'
cfg.PATHS.OUT_FOLDER = './OUT'

# Does the home dir have a config with additional param?
# Add them. Or override defaults defined here
if os.path.exists(os.path.join(cfg.DANE.HOME_DIR, "config.yml")):
    cfg.merge_from_file(os.path.join(cfg.DANE.HOME_DIR, "config.yml"))

# Does the file that is importing this, have a base_config.yml in its dir?
stack = [s for s in inspect.stack() if s.index is not None]
if len(stack) > 1:
    importing_path, _ = os.path.split(os.path.abspath(stack[1].filename))
else:
    # Import happening on CLI, so use cwd
    importing_path = os.getcwd()

if os.path.exists(os.path.join(importing_path, "base_config.yml")):
    cfg.merge_from_file(os.path.join(importing_path, "base_config.yml"))

# Does the cwd have a config with specific param?
if os.path.exists(os.path.join(os.getcwd(), "config.yml")):
    cfg.merge_from_file(os.path.join(os.getcwd(), "config.yml"))
elif cfg.CONFIG.REQUIRED:
    # base_config has indicated it requires a config.yml
    # as it wont or shouldnt run with default parameters.
    raise errors.ConfigRequiredError(
    "A config.yml is required that configures this component. " \
    "Please refer to https://dane.readthedocs.io/en/latest/intro.html#configuration "\
    "for more information.")

# make immutable
cfg.freeze()
