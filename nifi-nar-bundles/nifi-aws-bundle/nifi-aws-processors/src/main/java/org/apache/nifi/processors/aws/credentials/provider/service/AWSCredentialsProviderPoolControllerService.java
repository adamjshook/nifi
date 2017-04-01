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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsProviderFactory;
import org.apache.nifi.reporting.InitializationException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ACCESS_KEY;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_EXTERNAL_ID;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.CREDENTIALS_FILE;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.PROFILE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.SECRET_KEY;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.USE_ANONYMOUS_CREDENTIALS;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS;

/**
 * Implementation of AWSCredentialsProviderService interface
 *
 * @see AWSCredentialsProviderService
 */
@CapabilityDescription("Defines credentials for Amazon Web Services processors. " +
        "Uses default credentials without configuration. " +
        "Default credentials support EC2 instance profile/role, default user profile, environment variables, etc. " +
        "Additional options include access key / secret key pairs, credentials file, named profile, and assume role credentials.")
@Tags({ "aws", "credentials","provider" })
public class AWSCredentialsProviderPoolControllerService
        extends AbstractControllerService implements AWSCredentialsProviderPoolService {

    /*
     * AWS Role Arn used for cross account access
     *
     * @see <a href="http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#genref-arns">AWS ARN</a>
     */
    public static final PropertyDescriptor ASSUME_ROLE_ARN_EXPRESSION = new PropertyDescriptor.Builder()
            .name("Assume Role ARN")
            .displayName("Assume Role ARN")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .description("The AWS Role ARN for cross account access. This is used in conjunction with role name and session timeout")
            .build();

    /**
     * The role name while creating aws role
     */
    public static final PropertyDescriptor ASSUME_ROLE_NAME_EXPRESSION = new PropertyDescriptor.Builder()
            .name("Assume Role Session Name")
            .displayName("Assume Role Session Name")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .description("The AWS Role Name for cross account access. This is used in conjunction with role ARN and session time out")
            .build();

    /**
     * The maximum size of the Guava cache holding the credentials pool
     */
    public static final PropertyDescriptor CREDENTIALS_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("Max Pool Size")
            .displayName("Max Pool Size")
            .expressionLanguageSupported(false)
            .required(false)
            .defaultValue("10")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .sensitive(false)
            .description("Maximum size of the Guava cache holding the credentials pool")
            .build();

    /**
     * The expiration after access
     */
    public static final PropertyDescriptor CREDENTIALS_POOL_EXPIRATION_INTERVAL = new PropertyDescriptor.Builder()
            .name("Pool Expiration Interval")
            .displayName("Pool Expiration Interval")
            .expressionLanguageSupported(false)
            .required(false)
            .defaultValue("5 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .description("Expiration after access of the Guava cache holding the credentials pool")
            .build();

    public static final PropertyDescriptor MAX_SESSION_TIME = CredentialPropertyDescriptors.MAX_SESSION_TIME;

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(USE_DEFAULT_CREDENTIALS);
        props.add(ACCESS_KEY);
        props.add(SECRET_KEY);
        props.add(CREDENTIALS_FILE);
        props.add(PROFILE_NAME);
        props.add(USE_ANONYMOUS_CREDENTIALS);
        props.add(ASSUME_ROLE_ARN_EXPRESSION);
        props.add(ASSUME_ROLE_NAME_EXPRESSION);
        props.add(MAX_SESSION_TIME);
        props.add(ASSUME_ROLE_EXTERNAL_ID);
        props.add(ASSUME_ROLE_PROXY_HOST);
        props.add(ASSUME_ROLE_PROXY_PORT);
        props.add(CREDENTIALS_POOL_SIZE);
        props.add(CREDENTIALS_POOL_EXPIRATION_INTERVAL);
        properties = Collections.unmodifiableList(props);
    }

    private final CredentialsProviderFactory credentialsProviderFactory = new CredentialsProviderFactory();
    private volatile AWSCredentialsProviderPool credentialsProviderPool;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public AWSCredentialsProviderPool getCredentialsProviderPool() throws ProcessException {
        return credentialsProviderPool;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        return credentialsProviderFactory.validate(validationContext);
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        credentialsProviderPool = new AWSCredentialsProviderPool(credentialsProviderFactory, context);
    }

    @Override
    public String toString() {
        return "AWSCredentialsProviderPoolService[id=" + getIdentifier() + "]";
    }
}