# TODO: Implement Proper Error Handling. Status codes in body do not reflect header values


import subprocess
from fastapi import FastAPI, Header
from utils import util as utils
from enum import Enum
from bin.model.response import Response


class Parameters(Enum):
    EXECUTION_PARAMS = ['--request_data', '--rebuild']
    ARGUMENT = 'Y'


class ResponseCodes(Enum):
    SUCCESS_REBUILD = (200, "The rebuild completed successfully")
    SUCCESS_REQUEST = (200, "The request completed successfully")
    INVALID_REQUEST = (400, "Invalid Request")
    SERVER_ERROR = (500, "Server Error")


app = FastAPI()


@app.get("/auto_ml")
def model_output(execution_param: str, argument: str, token_id: str = Header(None)):
    u"""
    Executes the trained model on input user data, outputting an approval or denial, or executes a rebuild of the model,
    depending on the input parameters.

    :param execution_param: One of 3 possible values [--rebuild, --request_data, --hypertune].\n
    - "--rebuild" will retrain the model, storing the old model in archived_models and creating a new pickle file.\n
    - "--request_data" will execute the current model on the input data. Hypertune is not currently supported

    :param argument: Either 'Y' or a json string.\n
    - if execution_param is --rebuild, then must take on the value 'Y'\n
    - if execution_param is --request_data, then must be a json input with matching user values to each relevant key

    :param token_id: A form of verification, token_id HEADER must match TOKEN_ID present in config.ini in order to execute

    """
    general_properties = utils.collect_property_file_contents("../../properties/config.ini", 'GENERAL')

    # check input parameters before execution
    messages = []

    if token_id != general_properties["token_id"]:
        messages.append("Invalid token id. Validate the provided header (token_id) " +
                        "is present and matches the value in the config.ini")

    if execution_param not in Parameters.EXECUTION_PARAMS.value:
        messages.append(
            f"Invalid execution parameter (execution_param). Only {Parameters.EXECUTION_PARAMS.value} options are supported.")

    if execution_param == '--rebuild' and argument != Parameters.ARGUMENT.value:
        messages.append("Rebuild must have argument 'Y'")

    if len(messages) != 0:
        message = "; ".join(messages)
        response = Response(None, None, ResponseCodes.INVALID_REQUEST.value[0], message
                            , ResponseCodes.INVALID_REQUEST.value[1]).__str__()
        return eval(str(response))

    try:
        args = ['./run.sh', execution_param, argument]
        process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = process.communicate()
        return_code = process.returncode
        if execution_param == '--rebuild':
            if return_code == 0:
                final = result[0].decode("utf-8").strip("\n")
                if final != "":
                    response = Response(None, None, ResponseCodes.SUCCESS_REBUILD.value[0]
                                        , ResponseCodes.SUCCESS_REBUILD.value[1], None)
                else:
                    response = Response(None, None, ResponseCodes.SERVER_ERROR.value[0]
                                        , "The result is empty. Please verify the argument or look into the underlying rebuild process."
                                        , ResponseCodes.SERVER_ERROR.value[1])
            else:
                response = Response(None, None, ResponseCodes.SERVER_ERROR.value[0],
                                    "Return code was not 0. Please check underlying process."
                                    , ResponseCodes.SERVER_ERROR.value[1])
        elif execution_param == '--request_data':
            if return_code == 0:
                final = result[0].decode("utf-8").strip("\n")
                if final != "":
                    split_pos = final.find(']') + 1
                    res = eval(final[:split_pos].strip('[]'))
                    model = eval(final[split_pos + 1:])
                    response = Response(res, model, ResponseCodes.SUCCESS_REQUEST.value[0]
                                        , ResponseCodes.SUCCESS_REQUEST.value[1], None)
                else:
                    response = Response(None, None, 400, "The result is empty. Please verify the input for argument."
                                        , 'Invalid Request')
            else:
                response = Response(None, None, ResponseCodes.SERVER_ERROR.value[0]
                                    , "Return code was not 0. Please check underlying process."
                                    , ResponseCodes.SERVER_ERROR.value[1])

        return eval(str(response))
    except Exception as exc:
        response = Response(None, None, ResponseCodes.SERVER_ERROR.value[0], f"{exc}",
                            ResponseCodes.SERVER_ERROR.value[1])
        return eval(str(response))
