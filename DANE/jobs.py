import json
import sys
from abc import ABC, abstractmethod
import DANE
from DANE.utils import parse


class Job():
    """This is a class representation of a job in DANE, it holds both data 
    and logic.

    :param source_url: URL pointing to source material for this block
    :type source_url: str
    :param source_id: Id of the source object within the source collection
    :type source_id: str
    :param source_set: Identifier specifying the source collection the
        material is from.
    :type source_set: str
    :param tasks: A specification of the tasks to be performed
    :type source_set: class:`DANE.taskContainer`
    :type source_set: class:`DANE.Task`
    :type source_set: str
    :param job_id: ID of the job, assigned by DANE-core
    :type job_id: int, optional
    :param metadata: Dictionary containing metadata related to the job, or
        the source material
    :type metadata: dict, optional
    :param priority: Priority of the job in the task queue, defaults to 1
    :type priority: int, optional
    :param response: Dictionary containing results from other tasks
    :type response: dict, optional
    :param api: Reference to a class:`base_classes.base_handler` which is
        used to communicate with the database, and queueing system.
    :type api: :class:`base_classes.base_handler`, optional
    """

    def __init__(self, source_url, source_id, source_set, tasks,
            job_id=None, metadata={}, priority=1, response={}, api=None):
        # TODO add more input validation
        self.source_url = source_url
        self.source_id = source_id
        self.source_set = source_set
        self.api = api
        self.job_id = job_id

        if isinstance(tasks, str) or isinstance(tasks, dict):
            tasks = parse(tasks)
        elif not isinstance(tasks, DANE.taskContainer):
            raise TypeError("Tasks should be Task, taskContainer " + \
                    "subclass, or JSON serialised task_str")
        self.tasks = tasks
        self.tasks.set_api(self.api)

        self.metadata = metadata
        self.priority = priority
        self.response = response

    def __str__(self):
        return self.to_json()

    def to_json(self):
        """Returns this job serialised as JSON

        :return: JSON string of the job
        :rtype: str
        """
        astr = []
        for kw in vars(self):
            if kw == 'tasks':
                astr.append("\"tasks\" : {}".format(getattr(self, 
                    kw).to_json()))
            elif kw == 'api':
                continue
            else: 
                astr.append("\"{}\" : {}".format(kw, 
                    json.dumps(getattr(self, kw))))
        return "{{ {} }}".format(', '.join(astr))

    @staticmethod
    def from_json(json_str):
        """Constructs a :class:`DANE.Job` instance from a JSON string

        :param json_str: Serialised :class:`DANE.Job`
        :type json_str: str
        :return: JSON string of the job
        :rtype: :class:`DANE.Job`
        """
        data = json.loads(json_str)
        return Job(**data)

    def set_api(self, api):
        """Set the API for the job and all subtasks

        :param api: Reference to a :class:`base_classes.base_handler` which is
            used to communicate with the database, and queueing system.
        :type api: :class:`base_classes.base_handler`, optional
        :return: self
        """
        self.api = api
        for t in self.tasks:
            t.set_api(api)
        return self

    def register(self):
        """Register this job with DANE-core, this will assign a job_id to the
        job, and a task_id to all tasks. Requires an API to be set.

        :return: self
        """
        if self.job_id is not None:
            raise DANE.errors.APIRegistrationError('Job already registered')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    'register job')

        if 'SHARED' not in self.response.keys():
            self.response['SHARED'] = {}

        self.response['SHARED'].update(self.api.get_dirs(job=self))
        self.job_id = self.api.register_job(job=self)

        for t in self.tasks:
            t.register(job_id=self.job_id)

        self.api.propagate_task_ids(job=self)
        return self

    def refresh(self):
        """Retrieves the latest information for any fields that might have
        changed their values since the creation of this job. Requires an API
        to be set

        :return: self
        """
        if self.job_id is None:
            raise DANE.errors.APIRegistrationError(
                    'Cannot refresh unregistered job')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    'refresh job')

        job = self.api.jobFromJobId(self.job_id, get_state=True)
        self.tasks = job.tasks
        self.response = job.response
        self.metadata = job.metadata
        return self

    def apply(self, fn):
        """Applies `fn` to all :class:`DANE.Task` belonging to this job

        :param fn: Function handle in the form `fn(task)`
        :type fn: function
        :return: self
        """
        self.tasks.apply(fn)
        return self

    def run(self):
        """Run the tasks in this job.

        :return: self
        """
        self.tasks.run()
        return self

    def retry(self):
        """Try to run the tasks in this job again. Unlike 
        :func:`run` this will attempt to run tasks which 
        encountered an error state.

        :return: self
        """
        self.tasks.retry()
        return self

    def isDone(self):
        """Check if all tasks have completed.

        :return: Job doneness
        :rtype: bool
        """
        return self.tasks.isDone()
