# IMPORTATION STANDARD
from concurrent import futures
from types import MethodType

# IMPORTATION THIRD PARTY
import grpc

# IMPORTATION INTERNAL
from openbb_terminal.core.pb import converter
from openbb_terminal.core.pb.models.api_relay_pb2_grpc import (
    add_APIRelayServicer_to_server,
    APIRelayServicer,
)

class APIRelay(APIRelayServicer):
    @property
    def openbb(self):
        return self._openbb

    def get_operation_method(self, operation_path:str):
        menu_item_list = operation_path.split(".")
        operation_method = self._openbb

        if menu_item_list[0] == "openbb":
            menu_item_list = menu_item_list[1:]

        for menu_item in menu_item_list:
            operation_method = getattr(operation_method, menu_item)

        if not isinstance(operation_method, MethodType):
            raise Exception("Unknown `operation_method` type.")

        return operation_method

    def get_operation_result(self, operation_path:str, parameter_map: dict):
        operation_method = self.get_operation_method(operation_path=operation_path)

        return operation_method(**parameter_map)

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_APIRelayServicer_to_server(self, server)
        server.add_insecure_port("[::]:50051")
        server.start()

        print("STARTING :", self.__class__)

        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            print("KeyboardInterrupt")
            server.stop(grace=0)

    def __init__(self, openbb):
        self._openbb = openbb

    def operation_call(self, request, context):
        print("REQUEST :", request)
        print("REQUEST : type", type(request))

        operation_path = request.operation_path
        pb_parameter_map = request.parameter_map

        parameter_map = converter.message_to_dict(pb_parameter_map)

        result = self.get_operation_result(
            operation_path=operation_path,
            parameter_map=parameter_map,
        )

        response = converter.py_to_pb(obj=result)

        print("RESPONSE", response)
        print("RESPONSE : type", type(response))

        return response
