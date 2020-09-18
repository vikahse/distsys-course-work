# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from google.protobuf import any_pb2 as google_dot_protobuf_dot_any__pb2


class TestServerStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.AttachProcess = channel.stream_stream(
                '/TestServer/AttachProcess',
                request_serializer=google_dot_protobuf_dot_any__pb2.Any.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_any__pb2.Any.FromString,
                )


class TestServerServicer(object):
    """Missing associated documentation comment in .proto file."""

    def AttachProcess(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_TestServerServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'AttachProcess': grpc.stream_stream_rpc_method_handler(
                    servicer.AttachProcess,
                    request_deserializer=google_dot_protobuf_dot_any__pb2.Any.FromString,
                    response_serializer=google_dot_protobuf_dot_any__pb2.Any.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'TestServer', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class TestServer(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def AttachProcess(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/TestServer/AttachProcess',
            google_dot_protobuf_dot_any__pb2.Any.SerializeToString,
            google_dot_protobuf_dot_any__pb2.Any.FromString,
            options, channel_credentials,
            call_credentials, compression, wait_for_ready, timeout, metadata)
