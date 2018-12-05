#! /usr/bin/env python3
import argparse
import json
import logging
import logging.config
import os
import sys
import time
from concurrent import futures

import grpc

import ServerSideExtension_pb2 as SSE

import pyodbc
pyodbc.autocommit = True

conn = pyodbc.connect('DSN=Impala_ODBC', autocommit=True)
cursor = conn.cursor()

#from impala.dbapi import connect as impalaconnect
#impalaconn = impalaconnect(host='10.142.0.7', port=21050)
#impalacursor = impalaconn.cursor()

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class ExtensionService(SSE.ConnectorServicer):
    def __init__(self, funcdef_file):
        """
        Class initializer.
        :param funcdef_file: a function definition JSON file
        """
        self.function_definitions = funcdef_file
        if not os.path.exists('logs'):
            os.mkdir('logs')
        logging.config.fileConfig('logger.config')
        logging.info('Logging enabled')

    @property
    def functions(self):
        """
        :return: Mapping of function id and implementation
        """
        return {
            0: '_get_sql'
        }

    """
    Implementation of added functions.
    """

    @staticmethod
    def _get_sql(request):
        """
        GetSQL function, tensor
        """
        # Iterate over bundled rows
        for request_rows in request:
            response_rows = []
            # Iterating over rows
            for row in request_rows.rows:
                # Retrieve numerical value of parameter and append to the params variable
                # Length of param is 1 since one column is received, the [0] collects the first value in the list
                param = [d.strData for d in row.duals][0]

                print (param)

                # Execute SQL command
                cursor.execute(param)
                results = cursor.fetchall()
                results = results[0][0]
                # Create an iterable of dual with numerical value
                duals = iter([SSE.Dual(numData=results)])
                response_rows.append(SSE.Row(duals=duals))

            # Yield the row data constructed
            yield SSE.BundledRows(rows=response_rows)

    @staticmethod
    def _get_function_id(context):
        """
        Retrieve function id from header.
        :param context: context
        :return: function id
        """
        metadata = dict(context.invocation_metadata())
        header = SSE.FunctionRequestHeader()
        header.ParseFromString(metadata['qlik-functionrequestheader-bin'])

        return header.functionId

    """
    Implementation of rpc functions.
    """

    def GetCapabilities(self, request, context):
        """
        Get capabilities.
        Note that either request or context is used in the implementation of this method, but still added as
        parameters. The reason is that gRPC always sends both when making a function call and therefore we must include
        them to avoid error messages regarding too many parameters provided from the client.
        :param request: the request, not used in this method.
        :param context: the context, not used in this method.
        :return: the capabilities.
        """
        logging.info('GetCapabilities')

        # Create an instance of the Capabilities grpc message
        # Enable(or disable) script evaluation
        capabilities = SSE.Capabilities(allowScript=False)

        # If user defined functions supported, add the definitions to the message
        with open(self.function_definitions) as json_file:
            # Iterate over each function definition and add data to the Capabilities grpc message
            for definition in json.load(json_file)['Functions']:
                function = capabilities.functions.add()
                function.name = definition['Name']
                function.functionId = definition['Id']
                function.functionType = definition['Type']
                function.returnType = definition['ReturnType']

                # Retrieve name and type of each parameter
                for param_name, param_type in sorted(definition['Params'].items()):
                    function.params.add(name=param_name, dataType=param_type)

                logging.info('Adding to capabilities: {}({})'.format(function.name,
                                                                     [p.name for p in function.params]))

        return capabilities

    def ExecuteFunction(self, request_iterator, context):
        """
        Call corresponding function based on function id sent in header.
        :param request_iterator: an iterable sequence of RowData.
        :param context: the context.
        :return: an iterable sequence of RowData.
        """
        # Retrieve function id
        func_id = self._get_function_id(context)
        logging.info('ExecuteFunction (functionId: {})'.format(func_id))

        return getattr(self, self.functions[func_id])(request_iterator)

    """
    Implementation of the Server connecting to gRPC.
    """

    def Serve(self, port, key, certificate):
        """
        Server
        :param port: port to listen on.
        :param key: key to use for secure mode.
        :param certificate: certificate to use for secure mode.
        :return: None
        """
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        SSE.add_ConnectorServicer_to_server(self, server)

        if key and certificate:
            with open(key, 'rb') as f:
                private_key = f.read()
            with open(certificate, 'rb') as f:
                cert_chain = f.read()
            credentials = grpc.ssl_server_credentials([(private_key, cert_chain)])
            server.add_secure_port('[::]:{}'.format(port), credentials)
            logging.info('*** Running server in secure mode on port: {} ***'.format(port))
        else:
            server.add_insecure_port('[::]:{}'.format(port))
            logging.info('*** Running server in insecure mode on port: {} ***'.format(port))

        server.start()
        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            server.stop(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--key', nargs='?')
    parser.add_argument('--certificate', nargs='?')
    parser.add_argument('--port', nargs='?', default='50052')
    parser.add_argument('--definition-file', nargs='?', default='FuncDefs.json')
    args = parser.parse_args()

    # need to locate the file when script is called from outside it's location dir.
    def_file = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), args.definition_file)

    calc = ExtensionService(def_file)
    calc.Serve(args.port, args.key, args.certificate)
