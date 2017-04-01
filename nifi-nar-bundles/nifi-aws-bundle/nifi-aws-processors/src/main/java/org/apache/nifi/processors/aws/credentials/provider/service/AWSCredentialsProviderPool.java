/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.aws.credentials.provider.service;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsProviderFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderPoolControllerService.ASSUME_ROLE_ARN_EXPRESSION;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderPoolControllerService.ASSUME_ROLE_NAME_EXPRESSION;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderPoolControllerService.CREDENTIALS_POOL_SIZE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderPoolControllerService.CREDENTIALS_POOL_EXPIRATION_INTERVAL;

public class AWSCredentialsProviderPool {
    private final ConfigurationContext context;
    private final LoadingCache<Pair<String, String>, AWSCredentialsProvider> providerCache;

    public AWSCredentialsProviderPool(final CredentialsProviderFactory credentialsProviderFactory, final ConfigurationContext context) {
        this.context = context;

        providerCache = CacheBuilder.newBuilder()
                .maximumSize(context.getProperty(CREDENTIALS_POOL_SIZE).asInteger())
                .expireAfterAccess(context.getProperty(CREDENTIALS_POOL_EXPIRATION_INTERVAL).asTimePeriod(MILLISECONDS), MILLISECONDS)
                .build(new CacheLoader<Pair<String, String>, AWSCredentialsProvider>() {
                    @Override
                    public AWSCredentialsProvider load(Pair<String, String> key)
                            throws Exception {
                        Map<PropertyDescriptor, String> properties = new HashMap<>(context.getProperties());
                        properties.put(ASSUME_ROLE_ARN_EXPRESSION, key.getLeft());
                        properties.put(ASSUME_ROLE_NAME_EXPRESSION, key.getRight());
                        System.out.println("Creating new CredentialsProvider for: " + key);
                        return credentialsProviderFactory.getCredentialsProvider(properties);
                    }
                });
    }

    public AWSCredentialsProvider getCredentialsProvider(final FlowFile flowFile) {
        final String arn = context.getProperty(ASSUME_ROLE_ARN_EXPRESSION).evaluateAttributeExpressions(flowFile).getValue();
        final String name = context.getProperty(ASSUME_ROLE_NAME_EXPRESSION).evaluateAttributeExpressions(flowFile).getValue();
        try {
            return providerCache.get(Pair.of(arn, name));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
