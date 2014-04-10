/*
 * Copyright (c) 2013 - 2014, Allen A. George <allen dot george at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of libraft nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.libraft.kayvee.resources;

import com.google.common.util.concurrent.Futures;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.yammer.dropwizard.jersey.InvalidEntityExceptionMapper;
import com.yammer.dropwizard.jersey.JsonProcessingExceptionMapper;
import com.yammer.dropwizard.jersey.LoggingExceptionMapper;
import com.yammer.dropwizard.testing.ResourceTest;
import io.libraft.NotLeaderException;
import io.libraft.algorithm.StorageException;
import io.libraft.kayvee.KayVeeConfigurationFixture;
import io.libraft.kayvee.TestLoggingRule;
import io.libraft.kayvee.api.KeyValue;
import io.libraft.kayvee.api.SetValue;
import io.libraft.kayvee.configuration.ClusterMember;
import io.libraft.kayvee.mappers.IllegalArgumentExceptionMapper;
import io.libraft.kayvee.mappers.KayVeeExceptionMapper;
import io.libraft.kayvee.store.DistributedStore;
import io.libraft.kayvee.store.KeyAlreadyExistsException;
import io.libraft.kayvee.store.KeyNotFoundException;
import io.libraft.kayvee.store.ValueMismatchException;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import java.sql.SQLException;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// TODO (AG): Find a way to parameterize all the exception-checking tests so I don't have to write the same method n times
public final class KeyResourceTest extends ResourceTest {

    private static final String KEY = "KEY";
    private static final String REQUEST_URI = "/keys/" + KEY;
    private static final String EXPECTED_VALUE = "EXPECTED_VALUE";
    private static final String NEW_VALUE = "NEW_VALUE";

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyResourceTest.class);

    private final SetValue setValue = new SetValue();
    private final SetValue casValue = new SetValue();

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    private String self;
    private String leader;
    private String leaderUrl;
    private DistributedStore distributedStore;

    @Override
    protected void setUpResources() throws Exception {
        // setup the request parameters
        setValue.setNewValue(NEW_VALUE);

        casValue.setExpectedValue(EXPECTED_VALUE);
        casValue.setNewValue(NEW_VALUE);

        Set<ClusterMember> members = KayVeeConfigurationFixture.KAYVEE_CONFIGURATION.getClusterConfiguration().getMembers();

        // set our own id
        self = KayVeeConfigurationFixture.KAYVEE_CONFIGURATION.getClusterConfiguration().getSelf();

        // set the leader for this test
        for (ClusterMember member : members) {
            if (!member.getId().equals(self)) {
                leader = member.getId();
                leaderUrl = member.getKayVeeUrl();
                break;
            }
        }

        checkState(leader != null, "no leader set for test");

        // setup the HttpServletRequest
        HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        when(httpServletRequest.getRequestURI()).thenReturn(REQUEST_URI);

        // setup the DistributedStore
        distributedStore = mock(DistributedStore.class);

        // register all the providers and resources with the dropwizard ResourceTest

        // start off with our injector for HttpServletRequest
        addProvider(new HttpServletRequestInjector(httpServletRequest));
        // add the standard dropwizard mappers
        addProvider(new LoggingExceptionMapper<Throwable>() { });
        addProvider(new InvalidEntityExceptionMapper());
        addProvider(new JsonProcessingExceptionMapper());
        // add our exception mappers
        addProvider(KayVeeExceptionMapper.class);
        addProvider(IllegalArgumentExceptionMapper.class);
        // add the root resource
        addResource(new KeysResource(members, distributedStore)); // root resource; it'll return a KeyResource
    }

    //----------------------------------------------------------------------------------------------------------------//

    // GET

    @Test
    public void shouldReturnKeyValuePairOnGet() {
        when(distributedStore.get(KEY)).thenReturn(Futures.<KeyValue>immediateFuture(new KeyValue(KEY, EXPECTED_VALUE)));

        KeyValue keyValue = client().resource(REQUEST_URI).get(KeyValue.class);

        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(EXPECTED_VALUE));
    }

    @Test
    public void shouldReturnNotFoundIfGetFailsBecauseAKeyNotFoundExceptionIsThrown() {
        when(distributedStore.get(KEY)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new KeyNotFoundException(KEY)));

        assertThatValidGetRequestReturnsErrorStatus(ClientResponse.Status.NOT_FOUND);
    }

    @Test
    public void shouldReturnInternalServerErrorIfGetFailsBecauseADatabaseExceptionIsThrownInsideDistributedStore() {
        when(distributedStore.get(KEY)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new StorageException(new SQLException("DATABASE GONE"))));

        assertThatValidGetRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void shouldReturnInternalServerErrorIfGetFailsBecauseAnExceptionIsThrownInServiceCode() {
        when(distributedStore.get(KEY)).thenThrow(new IllegalStateException("service code terminated unexpectedly"));

        assertThatValidGetRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void shouldReturnServiceUnavailableWithNoLocationIfGetFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToNull() {
        when(distributedStore.get(KEY)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new NotLeaderException(self, null)));

        assertThatValidGetRequestReturnsErrorStatus(ClientResponse.Status.SERVICE_UNAVAILABLE);
    }

    @Test
    public void shouldReturnRedirectWithLocationIfGetFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToAValidLeader() {
        when(distributedStore.get(KEY)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new NotLeaderException(self, leader)));

        assertThatValidGetRequestReturnsErrorStatus(ClientResponse.Status.MOVED_PERMANENTLY);
    }

    @Test
    public void shouldReturnInternalServerErrorWithNoLocationIfGetFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToAnInvalidLeader() {
        when(distributedStore.get(KEY)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new NotLeaderException(self, "UNKNOWN_LEADER")));

        assertThatValidGetRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    private void assertThatValidGetRequestReturnsErrorStatus(ClientResponse.Status expectedStatus) {
        assertThatResourceMethodReturnsStatus(client().resource(REQUEST_URI), "GET", expectedStatus);
    }

    //----------------------------------------------------------------------------------------------------------------//

    // SET and CAS

    @Test
    public void shouldReturnBadRequestIfPutMethodIsMadeAndNeitherNewValueNorExpectedValueAreSetInSetValue() {
        SetValue emptyValue = new SetValue();
        assertThatResourceMethodReturnsStatus(
                client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).entity(emptyValue),
                "PUT",
                ClientResponse.Status.BAD_REQUEST);
    }

    //----------------------------------------------------------------------------------------------------------------//

    // SET

    @Test
    public void shouldCreateKeyValuePairOnSet() {
        when(distributedStore.set(KEY, setValue)).thenReturn(Futures.<KeyValue>immediateFuture(new KeyValue(KEY, NEW_VALUE)));

        KeyValue keyValue = client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).put(KeyValue.class, setValue);

        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));
    }

    @Test
    public void shouldReturnBadRequestIfSetFailsBecauseNewValueIsNull() {
        when(distributedStore.set(KEY, setValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new StorageException(new SQLException("DATABASE GONE"))));

        SetValue nulledValue = new SetValue();
        nulledValue.setNewValue(null);

        assertThatResourceMethodReturnsStatus(
                client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).entity(nulledValue),
                "PUT",
                ClientResponse.Status.BAD_REQUEST);
    }

    @Test
    public void shouldReturnInternalServerErrorIfSetFailsBecauseADatabaseExceptionIsThrownInsideDistributedStore() {
        when(distributedStore.set(KEY, setValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new StorageException(new SQLException("DATABASE GONE"))));

        assertThatValidSetRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void shouldReturnInternalServerErrorIfSetFailsBecauseAnExceptionIsThrownInServiceCode() {
        when(distributedStore.set(KEY, setValue)).thenThrow(new IllegalStateException("service code terminated unexpectedly"));

        assertThatValidSetRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void shouldReturnServiceUnavailableWithNoLocationIfSetFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToNull() {
        when(distributedStore.set(KEY, setValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new NotLeaderException(self, null)));

        assertThatValidSetRequestReturnsErrorStatus(ClientResponse.Status.SERVICE_UNAVAILABLE);
    }

    @Test
    public void shouldReturnRedirectWithLocationIfSetFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToAValidLeader() {
        when(distributedStore.set(KEY, setValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new NotLeaderException(self, leader)));

        assertThatValidSetRequestReturnsErrorStatus(ClientResponse.Status.MOVED_PERMANENTLY);
    }

    @Test
    public void shouldReturnInternalServerErrorWithNoLocationIfSetFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToAnInvalidLeader() {
        when(distributedStore.set(KEY, setValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new NotLeaderException(self, "UNKNOWN_LEADER")));

        assertThatValidSetRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    private void assertThatValidSetRequestReturnsErrorStatus(ClientResponse.Status expectedStatus) {
        assertThatResourceMethodReturnsStatus(
                client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).entity(setValue),
                "PUT",
                expectedStatus);
    }

    //----------------------------------------------------------------------------------------------------------------//

    // CAS

    @Test
    public void shouldReturnModifiedKeyValuePairIfCASSucceeds() {
        when(distributedStore.compareAndSet(KEY, casValue)).thenReturn(Futures.<KeyValue>immediateFuture(new KeyValue(KEY, NEW_VALUE)));

        KeyValue keyValue = client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).put(KeyValue.class, casValue);

        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));
    }

    @Test
    public void shouldReturnKeyValuePairIfCASSucceedsAndCreatesANewKey() {
        SetValue createKeySetValue = new SetValue();
        createKeySetValue.setExpectedValue(null);
        createKeySetValue.setNewValue(NEW_VALUE);

        when(distributedStore.compareAndSet(KEY, createKeySetValue)).thenReturn(Futures.<KeyValue>immediateFuture(new KeyValue(KEY, NEW_VALUE)));

        KeyValue keyValue = client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).put(KeyValue.class, createKeySetValue);

        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));
    }

    @Test
    public void shouldReturnNoContentIfCASSucceedsAndDeletesAKey() {
        SetValue deleteKeySetValue = new SetValue();
        deleteKeySetValue.setExpectedValue(EXPECTED_VALUE);
        deleteKeySetValue.setNewValue(null);

        when(distributedStore.compareAndSet(KEY, deleteKeySetValue)).thenReturn(Futures.<KeyValue>immediateFuture(null));

        ClientResponse response = client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).put(ClientResponse.class, deleteKeySetValue);

        assertThat(response.getClientResponseStatus(), equalTo(ClientResponse.Status.NO_CONTENT));
    }

    @Test
    public void shouldReturnConflictIfCompareAndSetFailsBecauseAValueMismatchExceptionIsThrown() {
        when(distributedStore.compareAndSet(KEY, casValue)).thenReturn(
                Futures.<KeyValue>immediateFailedFuture(new ValueMismatchException(KEY, EXPECTED_VALUE, "EXISTING_VALUE")));

        assertThatResourceMethodReturnsStatus(
                client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).entity(casValue),
                "PUT",
                ClientResponse.Status.CONFLICT
        );
    }

    @Test
    public void shouldReturnConflictIfCompareAndSetFailsBecauseAKeyAlreadyExistsExceptionIsThrown() {
        // we expect to be creating a new key
        final SetValue newKeyValue = new SetValue();
        newKeyValue.setExpectedValue(null);
        newKeyValue.setNewValue(NEW_VALUE);

        when(distributedStore.compareAndSet(KEY, newKeyValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new KeyAlreadyExistsException(KEY)));

        assertThatResourceMethodReturnsStatus(
                client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).entity(newKeyValue),
                "PUT",
                ClientResponse.Status.CONFLICT
        );
    }

    @Test
    public void shouldReturnNotFoundIfCompareAndSetFailsBecauseAKeyNotFoundExceptionIsThrown() {
        when(distributedStore.compareAndSet(KEY, casValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new KeyNotFoundException(KEY)));

        assertThatResourceMethodReturnsStatus(
                client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).entity(casValue),
                "PUT",
                ClientResponse.Status.NOT_FOUND
        );
    }

    @Test
    public void shouldReturnInternalServerErrorIfCompareAndSetFailsBecauseADatabaseExceptionIsThrownInsideDistributedStore() {
        when(distributedStore.compareAndSet(KEY, casValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new StorageException(new SQLException("DATABASE GONE"))));

        assertThatValidCompareAndSetRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void shouldReturnInternalServerErrorIfCompareAndSetFailsBecauseAnExceptionIsThrownInServiceCode() {
        when(distributedStore.compareAndSet(KEY, casValue)).thenThrow(new IllegalStateException("service code terminated unexpectedly"));

        assertThatValidCompareAndSetRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void shouldReturnServiceUnavailableWithNoLocationIfCompareAndSetFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToNull() {
        when(distributedStore.compareAndSet(KEY, casValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new NotLeaderException(self, null)));

        assertThatValidCompareAndSetRequestReturnsErrorStatus(ClientResponse.Status.SERVICE_UNAVAILABLE);
    }

    @Test
    public void shouldReturnRedirectWithLocationIfCompareAndSetFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToAValidLeader() {
        when(distributedStore.compareAndSet(KEY, casValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new NotLeaderException(self, leader)));

        assertThatValidCompareAndSetRequestReturnsErrorStatus(ClientResponse.Status.MOVED_PERMANENTLY);
    }

    @Test
    public void shouldReturnInternalServerErrorWithNoLocationIfCompareAndSetFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToAnInvalidLeader() {
        when(distributedStore.compareAndSet(KEY, casValue)).thenReturn(Futures.<KeyValue>immediateFailedFuture(new NotLeaderException(self, "UNKNOWN_LEADER")));

        assertThatValidCompareAndSetRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    private void assertThatValidCompareAndSetRequestReturnsErrorStatus(ClientResponse.Status expectedStatus) {
        assertThatResourceMethodReturnsStatus(
                client().resource(REQUEST_URI).type(MediaType.APPLICATION_JSON_TYPE).entity(casValue),
                "PUT",
                expectedStatus);
    }

    //----------------------------------------------------------------------------------------------------------------//

    // DEL

    @Test
    public void shouldCompleteSuccessfullyIfDistributedStoreDeleteCompletesSuccessfully() {
        when(distributedStore.delete(KEY)).thenReturn(Futures.<Void>immediateFuture(null));

        // jersey special-cases the situation where you specify
        // ClientResponse.class as the return type and returns the actual
        // response
        ClientResponse response = client().resource(REQUEST_URI).delete(ClientResponse.class);
        assertThat(response.getClientResponseStatus(), equalTo(ClientResponse.Status.NO_CONTENT));
    }

    @Test
    public void shouldReturnInternalServerErrorIfDeleteFailsBecauseADatabaseExceptionIsThrownInsideDistributedStore() {
        when(distributedStore.delete(KEY)).thenReturn(Futures.<Void>immediateFailedFuture(new StorageException(new SQLException("DATABASE GONE"))));

        assertThatValidDeleteRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void shouldReturnInternalServerErrorIfDeleteFailsBecauseAnExceptionIsThrownInServiceCode() {
        when(distributedStore.delete(KEY)).thenThrow(new IllegalStateException("service code terminated unexpectedly"));

        assertThatValidDeleteRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void shouldReturnServiceUnavailableWithNoLocationIfDeleteFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToNull() {
        when(distributedStore.delete(KEY)).thenReturn(Futures.<Void>immediateFailedFuture(new NotLeaderException(self, null)));

        assertThatValidDeleteRequestReturnsErrorStatus(ClientResponse.Status.SERVICE_UNAVAILABLE);
    }

    @Test
    public void shouldReturnRedirectWithLocationIfDeleteFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToAValidLeader() {
        when(distributedStore.delete(KEY)).thenReturn(Futures.<Void>immediateFailedFuture(new NotLeaderException(self, leader)));

        assertThatValidDeleteRequestReturnsErrorStatus(ClientResponse.Status.MOVED_PERMANENTLY);
    }

    @Test
    public void shouldReturnInternalServerErrorWithNoLocationIfDeleteFailsBecauseNotLeaderExceptionIsThrownWithTheLeaderSetToAnInvalidLeader() {
        when(distributedStore.delete(KEY)).thenReturn(Futures.<Void>immediateFailedFuture(new NotLeaderException(self, "UNKNOWN_LEADER")));

        assertThatValidDeleteRequestReturnsErrorStatus(ClientResponse.Status.INTERNAL_SERVER_ERROR);
    }

    private void assertThatValidDeleteRequestReturnsErrorStatus(ClientResponse.Status expectedStatus) {
        assertThatResourceMethodReturnsStatus(client().resource(REQUEST_URI), "DELETE", expectedStatus);
    }

    //----------------------------------------------------------------------------------------------------------------//

    private void assertThatResourceMethodReturnsStatus(final WebResource.Builder webResourceBuilder, final String method, ClientResponse.Status expectedStatus) {
        assertThatResourceMethodReturnsStatus(new Runnable() {

            @Override
            public void run() {
                webResourceBuilder.method(method);
            }
        }, expectedStatus);
    }

    private void assertThatResourceMethodReturnsStatus(final WebResource webResource, final String method, ClientResponse.Status expectedStatus) {
        assertThatResourceMethodReturnsStatus(new Runnable() {

            @Override
            public void run() {
                webResource.method(method);
            }
        }, expectedStatus);
    }

    private void assertThatResourceMethodReturnsStatus(Runnable webResourceRunnable, ClientResponse.Status expectedStatus) {
        UniformInterfaceException resourceException = null;
        try {
            webResourceRunnable.run();
        } catch (UniformInterfaceException e) {
            resourceException = e;
        }
        resourceException = checkNotNull(resourceException);

        ClientResponse errorResponse = resourceException.getResponse();

        assertThat(errorResponse.getClientResponseStatus(), equalTo(expectedStatus));

        if (expectedStatus == ClientResponse.Status.MOVED_PERMANENTLY) {
            assertThat(errorResponse.getLocation().toString(), equalTo(leaderUrl + REQUEST_URI));
        } else {
            assertThat(errorResponse.getLocation(), nullValue());
        }
    }
}
