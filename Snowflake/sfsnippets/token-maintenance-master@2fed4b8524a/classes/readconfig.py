import os
import configparser


class readConfig:

    def __init__(self):
        """
        Init function that initiates all the other functions
        """
        self.getconfigfile()
        self.getdefaultlogpath()
        self.snowflake_db_connect()

        
            
    def getconfigfile(self):
        """
        This function validates the path of config file and directory.
        :return:
        """
        currentFilePath = os.path.realpath(__file__)
        currentDirPath = os.path.dirname(currentFilePath)
        configDirPath = currentDirPath + "../../config"
        configDirPath = os.path.realpath(configDirPath)
        if(os.path.isdir(configDirPath)):
            self.configFile = os.path.join(configDirPath, "connection.ini")
            if not(os.path.exists(self.configFile)):
                print("Configuration File : [" + self.configFile + "] does not exits. Exiting.")
                exit(1)
            self.config = configparser.ConfigParser()
            self.config.read(self.configFile)
        else:
            print("Configuration File Directory: [" + configDirPath + "] does not exits. Exiting.")
            exit(1)

    def getconfigvalue(self, sectionname, keyname):
        """
        This function used to assign vaulue for each key .
        :param sectionname:
        :param keyname:
        :return:
        """
        if sectionname in self.config:
            configsection = self.config[sectionname]
            if keyname in configsection:
                returnvalue = configsection[keyname]
            else:
                returnvalue = ""
        else:
            print("Section: [" + sectionname + "] not found.")
            returnvalue = ""
        return returnvalue

    def getdefaultlogpath(self):
        """
        It assigns the default paths to respective variable
        :return:
        """
        self.defaultlogfilepath  = self.getconfigvalue("default_file_paths", "log_path")


    def snowflake_db_connect(self):
        """
        Key value pair assignment for snowflake connection details .
        :return:
        """
        self.keeperurl = self.getconfigvalue("snowflake_db_connection", "url")
        self.namespace = self.getconfigvalue("snowflake_db_connection", "namespace")
        self.token = self.getconfigvalue("snowflake_db_connection", "token")
        self.keeper_path = self.getconfigvalue("snowflake_db_connection", "keeper_path")
        self.user = self.getconfigvalue("snowflake_db_connection", "user")
        self.account = self.getconfigvalue("snowflake_db_connection", "account")
        self.warehouse = self.getconfigvalue("snowflake_db_connection", "warehouse")
        self.role = self.getconfigvalue("snowflake_db_connection", "role")
        self.database = self.getconfigvalue("snowflake_db_connection", "database")
        self.schema = self.getconfigvalue("snowflake_db_connection", "schema")
        self.api_url =self.getconfigvalue("snowflake_db_connection", "api_url")
        self.rotation_period = self.getconfigvalue("snowflake_db_connection", "rotation_period")

