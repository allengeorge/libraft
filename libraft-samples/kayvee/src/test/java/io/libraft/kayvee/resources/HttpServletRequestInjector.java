package io.libraft.kayvee.resources;

import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

/**
 * Injector (to be used for test only!) that supplies a singleton
 * instance for a parameter/property of the type:
 * <pre>
 *     @Context
 *     private HttpServletRequest request;
 * </pre>
 * <pre>
 *     public void myMethod(@Context HttpServletRequest request, ...) {
 *         // rest of method ...
 *     }
 * </pre>
 * This class provides a <strong>singleton</strong> instance of
 * {@code HttpServletRequest} only.
 */
final class HttpServletRequestInjector extends SingletonTypeInjectableProvider<Context, HttpServletRequest> {

    HttpServletRequestInjector(HttpServletRequest instance) {
        super(HttpServletRequest.class, instance);
    }
}
