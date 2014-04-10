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

package io.libraft.agent.configuration;

import io.libraft.algorithm.RaftConstants;

import javax.annotation.Nullable;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/**
 * Implementation of {@link ConstraintValidator} that validates
 * values annotated by {@link MinEntriesToSnapshot}.
 */
public final class MinEntriesToSnapshotValidator implements ConstraintValidator<MinEntriesToSnapshot, Integer> {

    private int ceil;

    @Override
    public void initialize(MinEntriesToSnapshot constraintAnnotation) {
        this.ceil = constraintAnnotation.ceil();
    }

    @Override
    public boolean isValid(@Nullable Integer value, ConstraintValidatorContext context) {
        // if nulls aren't allowed, annotate the parameter with a @NotNull annotation
        // see http://docs.jboss.org/hibernate/validator/5.1/reference/en-US/html/validator-customconstraints.html (S.6.1.2)
        if (value == null) {
            return true;
        }

        // ensure that it's (either disabled or > 0) and (<= 'ceil')
        boolean isValid = ((value.compareTo(RaftConstants.SNAPSHOTS_DISABLED) == 0) || (value.compareTo(0) == 1)) &&
                          ((value.compareTo(ceil) == -1) || (value.compareTo(ceil) == 0));

        // return a custom error message
        // FIXME (AG): I'm still not clear exactly how this message is interpolated
        if (!isValid) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate("value should be " + RaftConstants.SNAPSHOTS_DISABLED + " or > 0 and <= " + ceil).addConstraintViolation();
        }

        return isValid;
    }
}
