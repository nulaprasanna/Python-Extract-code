###########################################################################################################################################
#Description:   This script is used to rotate the keeper vault token and keep the token active in config file.                            #
#                                                                      .                                                                  #
#Usage:         This scripts accepts the below parameters:                                                                                #
#                                       1.job name (ex : Token_rotation ) this is for control-M job trigger                               #                                                   #
#Version = 1                                                                                                                              #
#Language : python                                                                                                                        #
#Property owned by : CISCO
# AUthor : Tech Mahindra
###########################################################################################################################################

import sys
import requests
import json
from datetime import datetime
import argparse
import os
import configparser
from classes.readconfig import readConfig
from classes.event_log import eventLog


class Rotate_token:
    """
    This Rotate_token is a main class for token rotation script 
    """
    def __init__(self):
        self.exitCode = 0
        self.initArguments()

        if(self.exitCode == 1):
            exit(self.exitCode)

        self.config = readConfig()
        self.log = eventLog(self.config, self.jobName)

        self.logInfo("------------------------------------------------------------------------------------------", False)
        self.logInfo("------------------------------------------------------------------------------------------", False)
        self.logInfo("Executing Token Rotation job.")
        self.logInfo("------------------------------------------------------------------------------------------", False)
        self.run_main()

        if(self.exitCode == 0):
            self.logInfo("sf-api job [" + self.jobName + "] completed sucessfully.")
        else:
            self.logInfo("sf-api job [" + self.jobName + "] has failed.")

        self.logInfo("------------------------------------------------------------------------------------------", False)
        self.logInfo("------------------------------------------------------------------------------------------", False)
        exit(self.exitCode)

    def initArguments(self):
        """
        This Function is for argument parsing which is a job name Trigged from control-M everyday once .
        """
        parser = argparse.ArgumentParser(add_help=True)

        required = parser.add_argument_group('required arguments')
        required.add_argument('-j', '--jobname', action='store', dest='jobName', metavar='" Job Name"', help='log Job Name', required=True)
        parser.add_argument('--version', action='version', version='%(prog)s 1.0')
        arguments = parser.parse_args()

        self.jobName = arguments.jobName.strip()

        if(self.jobName == ""):
            print("Token Rotation job Failed. Job Name not specified.")
            self.exitCode = 1



    def put_token(self,token):
        """
        put_token function is used to rotate the token by calling sf-api with put request.
        """

        payload = {'svc_account': self.svc_account,
                   'user_token': token,'sf_account':self.sf_account}


        response = requests.request("PUT", self.config.api_url, data = payload)

        return response.text.encode('utf8')

    def get_token(self,token):
        """
        get_token function is used to get the token details to local from api by sending get request to sf-api
        """
        self.logInfo("Running Get command  Sf-api to get the Tokens ")
        headers = {
            'user-token': token
        }

        response = requests.request("GET", self.url, headers=headers)
        #print(response.text.encode('utf8'))
        return response.text.encode('utf8')
    def create_new_token(self,token):
        """
        Creates token if the number of token is less than 2
        :param token:
        :return:
        """

        self.logInfo("------------------------------------------------------------------------------------------", False)
        self.logInfo("Creating second token..")
        self.logInfo("------------------------------------------------------------------------------------------", False)

        payload = {'svc_account': self.svc_account, 'user_token': token ,'sf_account':self.sf_account}
        response = requests.request("POST", self.config.api_url, data = payload)
        self.logInfo("second  token output ..["+str(response.text.encode('utf8'))+"]")






    def run_main(self):
        """
        Main function for token rotation .
        :return:
        """
        token=self.config.token
        self.svc_account=self.config.user
        self.sf_account =self.config.account
        self.rotation_period = int(self.config.rotation_period)
        self.logInfo("------------------------------------------------------------------------------------------", False)

        self.url = str(self.config.api_url)+"?sf_account="+str(self.sf_account)+"&svc_account="+str(self.svc_account)
        self.logInfo("Connecting Sf-api with url   [" +str( self.url )+ "]")
        self.logInfo("------------------------------------------------------------------------------------------", False)
        token_no=[]
        js=json.loads(self.get_token(token))
        for key, value in js['returnObject'].items():

            token_no.append(key)
        if len(token_no) < 2:
            self.create_new_token(token)
        if (js['returnObject']['Token1']['token'] == token):
            exp_time=js['returnObject']['Token1']['token_expiration_date']
            exp_tm=datetime.strptime(exp_time, "%Y-%m-%dT%H:%M:%S")
            no_of_days_left = (exp_tm - datetime.now()).days
            self.logInfo("Config file has Token Number 2")
            self.logInfo("------------------------------------------------------------------------------------------", False)
            self.logInfo("Number of days left   [" +str( no_of_days_left )+ "]")
            if (no_of_days_left < self.rotation_period):
                set_token = js['returnObject']['Token2']['token']
                self.logInfo("Number Of days lef is less than ["+str(self.rotation_period) +". Hence changing congig file with Tokan 2")
                self.config.config.set('snowflake_db_connection', 'token', set_token)
                with open(self.config.configFile,'w' ) as cfg:
                    self.config.config.write(cfg)
                #self.put_token(token)
            else :
                self.logInfo("This Token has valid for more than 10 days . Hence not changing anything ")


        elif ((js['returnObject']['Token2']['token'] == token)) :
            exp_time=js['returnObject']['Token2']['token_expiration_date']
            exp_tm=datetime.strptime(exp_time, "%Y-%m-%dT%H:%M:%S")
            no_of_days_left = (exp_tm - datetime.now()).days
            self.logInfo("Config file has Token Number 2")
            self.logInfo("------------------------------------------------------------------------------------------", False)
            self.logInfo("Number of days left   [" +str( no_of_days_left )+ "]")
            self.logInfo("------------------------------------------------------------------------------------------", False)
            if (no_of_days_left < self.rotation_period):
                set_token = js['returnObject']['Token1']['token']
                self.logInfo("Number Of days lef is less than ["+str(self.rotation_period)+" . Hence changing congig file with Tokan 1")
                self.config.config.set('snowflake_db_connection', 'token', set_token)
                with open(self.config.configFile,'w' ) as cfg:
                    self.config.config.write(cfg)

                #self.put_token(token)
            else :
                self.logInfo("This Token has valid for more than 10 days . Hence not changing anything ")

        else:
            self.logInfo("No Valid Token in Config file . Please check ")
            exit(1)
            self.exist=1







    def logMessage(self, type, message, customFormat = True):
        if(self.log == None):
            print(message)
        else:
            self.log.logMessage(type, message, customFormat)

    def logInfo(self, message, customFormat = True):
        self.logMessage("INFO", message, customFormat)

    def logError(self, message, customFormat = True):
        self.logMessage("ERROR", message, customFormat)
if __name__== "__main__":
    Rotate_token()

