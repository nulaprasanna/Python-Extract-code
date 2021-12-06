Token Manager

Introduction

This utility helps with keeping Snowflake connectivity tokens up-to-date by replacing those, which are
about to expire with the latest tokens and rotating the expired tokens.

Getting Started

Clone this utility onto your local machine

	$ git clone https://bitbucket-eng-sjc1.cisco.com/bitbucket/projects/CGW/repos/token-manager/browse
	$ cd config

Add the full paths to the tokens you need rotate/replace to the files hosts.txt in the following format:

    [dataloader_dev]
    user = cgwedwadm
    host = cloudgw-dev-01
    svc_account = edw_datalake_svc
    file = /users/cgwedwadm/data_loader/config/ciscodev.us-east-1.conf
    key = TOKEN
    delimiter = ';'

Running the utility

To run this utility, use Python 3

    python token_manager.py
