@author: armanick 
# Token Rotation

How to trigger the script :

Python token_rotate.py -j token_rotation

Functionality :

•	This script checks the token available in our configuration file .
•	If number of token is less than 2 , than creates second token .
•	If token available in configuration file is about to expire (ex 10 days), it replace the token with other token and rotates the old token .


Note : first token and second token should have minimum 15 days gap .
