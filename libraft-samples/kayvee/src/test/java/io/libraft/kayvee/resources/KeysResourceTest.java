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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.yammer.dropwizard.jersey.InvalidEntityExceptionMapper;
import com.yammer.dropwizard.jersey.JsonProcessingExceptionMapper;
import com.yammer.dropwizard.jersey.LoggingExceptionMapper;
import com.yammer.dropwizard.testing.ResourceTest;
import io.libraft.NotLeaderException;
import io.libraft.kayvee.KayVeeConfigurationFixture;
import io.libraft.kayvee.TestLoggingRule;
import io.libraft.kayvee.api.KeyValue;
import io.libraft.kayvee.configuration.ClusterMember;
import io.libraft.kayvee.mappers.IllegalArgumentExceptionMapper;
import io.libraft.kayvee.mappers.KayVeeExceptionMapper;
import io.libraft.kayvee.store.DistributedStore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static org.fest.util.Preconditions.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// TODO (AG): find some way of combining the setup code in here with that in KeyResourceTest
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public final class KeysResourceTest extends ResourceTest {

    private static final String REQUEST_URI = "/keys";

    private static final Logger LOGGER = LoggerFactory.getLogger(KeysResourceTest.class);

    // Technique used from:
    // http://mjlex.blogspot.com/2012/04/writing-lightweight-rest-integration.html
    //
    // Also see:
    // https://java.net/projects/jersey/sources/svn/content/trunk/jersey/jersey-tests/src/test/java/com/sun/jersey/impl/inject/AnnotationInjectableTest.java?rev=5822
    //
    // Simply registering a resource or provider with injected properties (such as @Context-annotated HttpServletRequest)
    // will fail with the following error:
    //
    // 11:06:20.171 [main] INFO  c.s.j.t.f.s.c.i.InMemoryTestContainerFactory$InMemoryTestContainer - Creating low level InMemory test container configured at the base URI http://localhost:9998/
    // 11:06:20.456 [main] INFO  c.s.j.t.f.s.c.i.InMemoryTestContainerFactory$InMemoryTestContainer - Starting low level InMemory test container
    // 11:06:20.467 [main] INFO  c.s.j.s.i.a.WebApplicationImpl - Initiating Jersey application, version'Jersey: 1.17.1 02/28/2013 12:47 PM'
    // 11:06:20.605 [main] DEBUG org.eclipse.jetty.util.log - Logging to Logger[org.eclipse.jetty.util.log] via org.eclipse.jetty.util.log.Slf4jLog
    // 11:06:20.763 [main] ERROR com.sun.jersey.spi.inject.Errors - The following errors and warnings have been detected with resource and/or provider classes:
    //   SEVERE: Missing dependency for field: protected javax.servlet.http.HttpServletRequest io.libraft.kayvee.mappers.KayVeeLoggingExceptionMapper.request  <<<<<<------------ RELEVANT
    // 11:06:20.764 [main] INFO  c.s.j.t.f.s.c.i.InMemoryTestContainerFactory$InMemoryTestContainer - Stopping low level InMemory test container
    //
    // Most posts will simply recommend that you use the grizzly container. While this is
    // acceptable, it's overkill for lightweight unit tests.
    //
    // There is an alternative.
    //
    // The key is realizing that InMemoryContainer doesn't care that the field is annotated with an @Context annotation,
    // and that the property is usually injected by the Servlet container! It simply cares that it can't instantiate
    // HttpServletRequest, or find a provider that will instantiate one. Once you realize that, you can simply create an
    // injector that does the job and supplies an object of that type for you.
    //
    // Summary:
    // 1. Create a subclass of SingletonTypeInjectableProvider that recognizes @Context-annotated HttpServletRequest properties
    // 2. Register the provider with the ResourceTest (which in-turn adds it to the jersey config)
    //    using "addProvider(new SubClassOfSingletonTypeInjectableProvider())"
    // 3. Profit
    //
    // @see com.sun.jersey.spi.inject.InjectableProvider
    // @see com.sun.jersey.spi.inject.SingletonTypeInjectableProvider

    private String self;
    private String leader;
    private String leaderUrl;
    private DistributedStore distributedStore;
    private KeysResource keysResource;

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Override
    protected void setUpResources() throws Exception {
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

        // setup the resource
        distributedStore = mock(DistributedStore.class);
        keysResource = new KeysResource(members, distributedStore);

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
        // finally, add our resource
        addResource(keysResource);
    }

    @Test
    public void shouldReturnNewKeyResourceWhenForKeyIsCalled() {
        KeyResource keyResource = keysResource.forKey("key");
        assertThat(keyResource.getKey(), equalTo("key"));
    }

    @Test
    public void shouldReturnACollectionOfKeyValuePairsWhenGetIsCalled() {
        final List<KeyValue> serverAll = ImmutableList.of(
                new KeyValue("KEY1", "VALUE1"),
                new KeyValue("KEY2", "VALUE2"),
                new KeyValue("KEY3", "VALUE3")
        );

        when(distributedStore.getAll()).thenReturn(Futures.<Collection<KeyValue>>immediateFuture(serverAll));

        Collection<KeyValue> clientAll = client().resource(REQUEST_URI).get(new GenericType<Collection<KeyValue>>() { });

        assertThat(clientAll, containsInAnyOrder(serverAll.toArray()));
    }

    @Test
    public void shouldReturnInternalServerErrorIfAnErrorIsThrownInServiceCode() {
        when(distributedStore.getAll()).thenThrow(new IllegalStateException());

        ClientResponse response = getClientResponseForUnsuccessfulRequest();

        assertThat(response.getClientResponseStatus(), equalTo(ClientResponse.Status.INTERNAL_SERVER_ERROR));
        assertThat(response.getLocation(), nullValue());
    }

    @Test
    public void shouldReturnInternalServerErrorIfThereIsAnErrorInMakingTheDistributedStoresCall() {
        when(distributedStore.getAll()).thenReturn(Futures.<Collection<KeyValue>>immediateFailedFuture(new IllegalStateException()));

        ClientResponse response = getClientResponseForUnsuccessfulRequest();

        assertThat(response.getClientResponseStatus(), equalTo(ClientResponse.Status.INTERNAL_SERVER_ERROR));
        assertThat(response.getLocation(), nullValue());
    }

    @Test
    public void shouldReturnServiceUnavailableWithNoLocationWhenNotLeaderExceptionIsThrownAndTheLeaderIsNull() {
        when(distributedStore.getAll()).thenReturn(Futures.<Collection<KeyValue>>immediateFailedFuture(new NotLeaderException(self, null)));

        ClientResponse response = getClientResponseForUnsuccessfulRequest();

        assertThat(response.getClientResponseStatus(), equalTo(ClientResponse.Status.SERVICE_UNAVAILABLE));
        assertThat(response.getLocation(), nullValue());
    }

    @Test
    public void shouldReturnRedirectWithLocationWhenNotLeaderExceptionIsThrownAndTheLeaderIsSetToAValidLeader() {
        when(distributedStore.getAll()).thenReturn(Futures.<Collection<KeyValue>>immediateFailedFuture(new NotLeaderException(self, leader)));

        ClientResponse response = getClientResponseForUnsuccessfulRequest();

        assertThat(response.getClientResponseStatus(), equalTo(ClientResponse.Status.MOVED_PERMANENTLY));
        assertThat(response.getLocation().toString(), equalTo(leaderUrl + REQUEST_URI));
    }

    @Test
    public void shouldReturnInternalServerErrorWhenNotLeaderExceptionIsThrownAndTheLeaderIsSetToAnInvalidLeader() {
        when(distributedStore.getAll()).thenReturn(Futures.<Collection<KeyValue>>immediateFailedFuture(new NotLeaderException(self, "UNKNOWN_LEADER")));

        ClientResponse response = getClientResponseForUnsuccessfulRequest();

        assertThat(response.getClientResponseStatus(), equalTo(ClientResponse.Status.INTERNAL_SERVER_ERROR));
        assertThat(response.getLocation(), nullValue());
    }

    private ClientResponse getClientResponseForUnsuccessfulRequest() {
        UniformInterfaceException resourceException = null;

        try {
            client().resource(REQUEST_URI).get(Collection.class);
        } catch (UniformInterfaceException e) {
            resourceException = e;
        }

        resourceException = checkNotNull(resourceException);
        return resourceException.getResponse();
    }
}