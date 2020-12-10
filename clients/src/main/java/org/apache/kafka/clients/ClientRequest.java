package org.apache.kafka.clients;

import org.apache.kafka.common.requests.RequestSend;

/**
 * A request being sent to the server. This holds both the network send as well as the client-level metadata.
 * 这个类的作用是啥？
 */
public final class ClientRequest {

    private final long createdTimeMs;
    private final boolean expectResponse;
    private final RequestSend request;
    private final RequestCompletionHandler callback;
    private final boolean isInitiatedByNetworkClient;
    private long sendTimeMs;

    /**
     * @param createdTimeMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param request The request
     * @param callback A callback to execute when the response has been received (or null if no callback is necessary)
     */
    public ClientRequest(long createdTimeMs, boolean expectResponse, RequestSend request,
                         RequestCompletionHandler callback) {
        this(createdTimeMs, expectResponse, request, callback, false);
    }

    /**
     * @param createdTimeMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param request The request
     * @param callback A callback to execute when the response has been received (or null if no callback is necessary)
     * @param isInitiatedByNetworkClient Is request initiated by network client, if yes, its
     *                                   response will be consumed by network client
     */
    public ClientRequest(long createdTimeMs, boolean expectResponse, RequestSend request,
                         RequestCompletionHandler callback, boolean isInitiatedByNetworkClient) {
        this.createdTimeMs = createdTimeMs;
        this.callback = callback;
        this.request = request;
        this.expectResponse = expectResponse;
        this.isInitiatedByNetworkClient = isInitiatedByNetworkClient;
    }

    @Override
    public String toString() {
        return "ClientRequest(expectResponse=" + expectResponse +
            ", callback=" + callback +
            ", request=" + request +
            (isInitiatedByNetworkClient ? ", isInitiatedByNetworkClient" : "") +
            ", createdTimeMs=" + createdTimeMs +
            ", sendTimeMs=" + sendTimeMs +
            ")";
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public RequestSend request() {
        return request;
    }

    public boolean hasCallback() {
        return callback != null;
    }

    public RequestCompletionHandler callback() {
        return callback;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public boolean isInitiatedByNetworkClient() {
        return isInitiatedByNetworkClient;
    }

    public long sendTimeMs() {
        return sendTimeMs;
    }

    public void setSendTimeMs(long sendTimeMs) {
        this.sendTimeMs = sendTimeMs;
    }
}
