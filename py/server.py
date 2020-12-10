import logging
from concurrent.futures import ThreadPoolExecutor

import grpc
import numpy as np

from outliers_pb2 import OutliersResponse
from outliers_pb2_grpc import OutliersServicer, add_OutliersServicer_to_server


def find_outliers(data):
    out = np.where(np.abs(data - data.mean()) > 2 * data.std())
    return out[0]


class OutliersServer(OutliersServicer):

    def Detect(self, request, context):
        logging.info('delete request size: %s', len(request))
        data = np.fromiter((m.value for m in request.metrics), dtype='float64')
        indices = find_outliers(data)
        logging.info('found %d outliers', len(indices))
        resp = OutliersResponse(indices=indices)
        return resp


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    server = grpc.server(ThreadPoolExecutor())
    add_OutliersServicer_to_server(OutliersServer(), server)
    port = 9999
    server.add_insecure_port('[::]:%s' % port)
    server.start()
    logging.info('server ready on port %s', port)
    server.wait_for_termination()
