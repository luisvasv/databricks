import json
import os
import pandas as pd
from datetime import datetime
from tabulate import tabulate


class WorkflowException(Exception):
    """
    Base WorkflowException, we have this exception hierarchy:
      WorkflowException
        - ApplicationException (5xx errors)
            - DBException
                - DBConnectionError
        - ClientException
            - NotFoundError
            - NotAuthorizedError
    """

    def __init__(
        self, message: str, flow_name: str, exception_type: str = "WorkflowException"
    ) -> None:
        super().__init__(message)
        self.__error = message
        self.__flow_name = flow_name
        self.__exception_type = exception_type
        self.__info = None

    def __call__(self, traceback) -> None:
        self.__traceback__ = traceback

    def __format__(self):
        if self.__info is None:
            self.__info = {
                "flow_name": self.__flow_name,
                "date": datetime.utcnow().isoformat(),
                "file_name": os.path.split(self.__traceback__.tb_frame.f_code.co_filename)[1],
                "line_no": self.__traceback__.tb_lineno,
                "exception_type": self.__exception_type,
                "exception_message": self.__error,
            }
        instance = str(id(self))
        ast = "*" * 100
        message = f"""
  {ast}
  instance ID     --> {instance}
  flow name       --> {self.__info['flow_name']}
  date            --> {self.__info['date']}
  file error      --> {self.__info['file_name']}
  line #          --> {self.__info['line_no']}
  exception type  --> {self.__info['exception_type']}
  exception error --> {self.__info['exception_message']}
  {ast}
  """
        return message

    def __str__(self):
        return self.__format__()

    def _to_json(self):
        self.__format__()
        return json.dumps(self.__info)

    def _dict(self):
        self.__format__()
        return self.__info

    def _to_table(self):
        self.__format__()
        df = pd.DataFrame(self.__info, index=[0])
        return tabulate(df, headers="keys", tablefmt="psql")
        # return "[functionality desabled]"


class RealDemoException(WorkflowException):
    """
    Databrikcs No Data Exception
    """