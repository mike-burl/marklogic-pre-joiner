package com.marklogic.dataservices.prejoiner;

// IMPORTANT: Do not edit. This file is generated.

import com.marklogic.client.io.Format;
import java.io.Reader;


import com.marklogic.client.DatabaseClient;

import com.marklogic.client.impl.BaseProxy;

/**
 * Provides a set of operations on the database server
 */
public interface materializeDS {
    /**
     * Creates a materializeDS object for executing operations on the database server.
     *
     * The DatabaseClientFactory class can create the DatabaseClient parameter. A single
     * client object can be used for any number of requests and in multiple threads.
     *
     * @param db	provides a client for communicating with the database server
     * @return	an object for session state
     */
    static materializeDS on(DatabaseClient db) {
        final class materializeDSImpl implements materializeDS {
            private BaseProxy baseProxy;

            private materializeDSImpl(DatabaseClient dbClient) {
                baseProxy = new BaseProxy(dbClient, "/dataservices/test/materializeDS/");
            }

            @Override
            public Reader materializeClaims(String claimId, Long customerId) {
              return BaseProxy.XmlDocumentType.toReader(
                baseProxy
                .request("materializeClaims.xqy", BaseProxy.ParameterValuesKind.MULTIPLE_ATOMICS)
                .withSession()
                .withParams(
                    BaseProxy.atomicParam("claimId", false, BaseProxy.StringType.fromString(claimId)),
                    BaseProxy.atomicParam("customerId", false, BaseProxy.UnsignedLongType.fromLong(customerId))
                 )
                .withMethod("POST")
                .responseSingle(false, Format.XML)
                );
            }
        }

        return new materializeDSImpl(db);
    }

  /**
   * Invokes the materializeClaims operation on the database server
   *
   * @param claimId	provides input
   * @param customerId	provides input
   * @return	as output
   */
    Reader materializeClaims(String claimId, Long customerId);

}
