import os

from environs import Env


class Config:
    def __init__(self,):
        """
        Initializing enviromental variables and dag directory
        """
        self.dir_env = os.path.dirname(os.path.realpath(__file__)) + "/.env"
        self.dir_dag_template = os.path.dirname(os.path.realpath(__file__)) + "/sql"
        self.params = None
        self.env = Env()
        self.env.read_env(self.dir_env)

    def dir_dag_template(self):
        """
        return dag directory as string
        :return:
        :rtype:
        """
        return self.dir_dag_template
