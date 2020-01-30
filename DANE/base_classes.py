import DANE
from abc import ABC, abstractmethod
import pika
import json
import threading
import functools

class base_worker(ABC):
    """Abstract base class for a worker. 

    This class contains most of the logic of dealing with DANE-server,
    classes (workers) inheriting from this class only need to specific the
    callback method.
    
    :param queue: Name of the queue for this worker
    :type queue: str
    :param binding_key: A string following the format as explained
        here: https://www.rabbitmq.com/tutorials/tutorial-five-python.html
        or a list of such strings
    :type binding_key: str or list
    :param config: Config settings of the worker
    :type config: dict
    """
    def __init__(self, queue, binding_key, config):
        self.queue = queue
        self.binding_key = binding_key

        self.config = config
        self.host = config['RABBITMQ']['host']
        self.port = config['RABBITMQ']['port']
        self.exchange = config['RABBITMQ']['exchange']

        user = config['RABBITMQ']['user']
        password = config['RABBITMQ']['password']

        credentials = pika.PlainCredentials(user, password)
        self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        credentials=credentials,
                        host=self.host, port=self.port))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self.exchange, 
                exchange_type='topic')

        self.channel.queue_declare(queue=self.queue, 
                arguments={'x-max-priority': 10},
                durable=True)

        if not isinstance(self.binding_key, list):
            self.binding_key = [self.binding_key]

        for bk in self.binding_key:
            self.channel.queue_bind(exchange=self.exchange, 
                queue=self.queue,
                routing_key=bk)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(on_message_callback=self._callback, 
                                   auto_ack=False,
                                   queue=self.queue)

    def run(self):
        """Start listening for tasks to be executed.
        """
        self.channel.start_consuming()

    def stop(self):
        """Stop listening for tasks to be executed.
        """
        self.channel.stop_consuming()

    def _callback(self, ch, method, props, body):
        try:
            job = DANE.Job.from_json(body)
        except TypeError as e:
            response = { 'state': 400, 
                    'message': 'Invalid job format, unable to proceed'}

            self._ack_and_reply(json.dumps(response), ch, method, props)
        except Exception as e:
            response = { 'state': 500, 
                    'message': 'Unhandled error: ' + str(e)}

            self._ack_and_reply(json.dumps(response), ch, method, props)
        else: 
            self.thread = threading.Thread(target=self._run, 
                    args=(job, ch, method, props))
            self.thread.setDaemon(True)
            self.thread.start()

    def _run(self, job, ch, method, props):
        response = self.callback(job)

        if not isinstance(response, str):
            response = json.dumps(response)

        reply_cb = functools.partial(self._ack_and_reply, response,
                ch, method, props)
        self.connection.add_callback_threadsafe(reply_cb)

    def _ack_and_reply(self, response, ch, method, props):
        ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(
                         correlation_id = props.correlation_id,
                         delivery_mode=2),
                     body=str(response))

        ch.basic_ack(delivery_tag=method.delivery_tag)

    @abstractmethod
    def callback(self, job_request):
        """Function containing the core functionality that is specific to
        a worker. 

        :param job_request: Job specification for new task
        :type job_request: :class:`DANE.Job`
        :return: Task response with the `message`, `state`, and
            optional additional response information
        :rtype: dict or str
        """
        return

class base_handler(ABC):
    """Abstract base class for a handler. 

    A handler functions as the API used in DANE to facilitate all communication
    with the database and the queueing system. 
    
    :param config: Config settings for the handler
    :type config: dict
    """
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def register_job(self, job):
        """Register a job in the database

        :param job: The job
        :type job: :class:`DANE.Job`
        :return: job_id
        :rtype: int
        """
        return

    @abstractmethod
    def propagate_task_ids(self, job):
        """The task list is updated to include task_ids by the registration,
        propagate this change to the underlying database.

        :param job: The job
        :type job: :class:`DANE.Job`
        """
        return

    @abstractmethod
    def get_dirs(self, job):
        """This function returns the TEMP and OUT directories for this job
        creating them if they do not yet exist
        output should be stored in response['SHARED']
        
        :param job: The job
        :type job: :class:`DANE.Job`
        :return: Dict with keys `TEMP_FOLDER` and `OUT_FOLDER`
        :rtype: dict
        """
        return

    @abstractmethod
    def register(self, job_id, task):
        """Register a task in the database

        :param job_id: id of the job this task belongs to
        :type job_id: int
        :param task: the task to register
        :type task: :class:`DANE.Task`
        :return: task_id
        :rtype: int
        """
        return
    
    @abstractmethod
    def getTaskState(self, task_id):
        """Retrieve state for a given task_id

        :param task_id: id of the task
        :type task_id: int
        :return: task_state
        :rtype: int
        """
        return

    @abstractmethod
    def getTaskKey(self, task_id):
        """Retrieve task_key for a given task_id

        :param task_id: id of the task
        :type task_id: int
        :return: task_key
        :rtype: str
        """
        return

    @abstractmethod
    def jobFromJobId(self, job_id):
        """Construct and return a :class:`DANE.Job` given a job_id
        
        :param job_id: The id for the job
        :type job_id: int
        :return: The job
        :rtype: :class:`DANE.Job`
        """
        return

    @abstractmethod
    def jobFromTaskId(self, task_id):
        """Construct and return a :class:`DANE.Job` given a task_id
        
        :param task_id: The id of a task
        :type task_id: int
        :return: The job
        :rtype: :class:`DANE.Job`
        """
        return

    def isDone(self, task_id):
        """Verify whether a task is done.

        Doneness is determined by whether or not its state is `200`.
        
        :param task_id: The id of a task
        :type task_id: int
        :return: Task doneness
        :rtype: bool
        """
        return self.getTaskState(task_id) == 200

    @abstractmethod
    def run(self, task_id):
        """Run the task with this id, and change its task state to `102`.

        Running a task involves submitting it to a queue, so results might
        only be available much later. Expects a task to have state `201`,
        and it may retry tasks with state `502` or `503`.
        
        :param task_id: The id of a task
        :type task_id: int
        """
        return

    @abstractmethod
    def retry(self, task_id):
        """Retry the task with this id, and change its task state to `102`.

        Attempts to run a task which previously might have crashed. 
        
        :param task_id: The id of a task
        :type task_id: int
        """
        return

    @abstractmethod
    def callback(self, task_id, response):
        """Function that is called once a task gives back a response.

        This updates the state and response of the task in the database,
        and then calls :func:`DANE.Job.run()` to trigger the next task.

        :param task_id: The id of a task
        :type task_id: int
        :param response: Task response, should contain at least the `state`
            and a `message`
        :type response: dict
        """
        return

    @abstractmethod
    def search(self, source_id, source_set=None):
        """Returns jobs which exist for this source material.

        :param source_id: The id of the source material
        :type source_id: int
        :param source_set: Specific source material collection to search n
        :type source_set: str, optional
        :return: ids of found jobs
        :rtype: dict
        """
        return

    @abstractmethod
    def getUnfinished(self):
        """Returns jobs which are not finished and not queue, i.e., not state
        `102` or `200` 

        :return: ids of found jobs
        :rtype: dict
        """
        return