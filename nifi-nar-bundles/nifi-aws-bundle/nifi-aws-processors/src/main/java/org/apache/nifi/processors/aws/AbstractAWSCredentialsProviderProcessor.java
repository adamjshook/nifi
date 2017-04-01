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
package org.apache.nifi.processors.aws;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderPool;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderPoolService;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Base class for aws processors that uses AWSCredentialsProvider interface for creating aws clients.
 *
 * @param <ClientType> client type
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
 */
public abstract class AbstractAWSCredentialsProviderProcessor<ClientType extends AmazonWebServiceClient>
    extends AbstractAWSProcessor<ClientType>  {

    private AWSCredentialsProviderPool providerPool;

    /**
     * AWS credentials provider service
     *
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("AWS Credentials Provider service")
            .description("The Controller Service that is used to obtain aws credentials provider")
            .required(false)
            .identifiesControllerService(AWSCredentialsProviderService.class)
            .build();

    /**
     * AWS credentials provider pool service
     *
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_POOL_SERVICE = new PropertyDescriptor.Builder()
            .name("AWS Credentials Provider Pool service")
            .description("The Controller Service that is used to dynamically obtain AWS credentials provider by FlowFile attributes")
            .required(false)
            .identifiesControllerService(AWSCredentialsProviderPoolService.class)
            .build();

    /**
     * This method checks if {#link {@link #AWS_CREDENTIALS_PROVIDER_SERVICE} is available and if it
     * is, uses the credentials provider, otherwise it invokes the {@link AbstractAWSProcessor#onScheduled(ProcessContext)}
     * which uses static AWSCredentials for the aws processors
     */
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        ControllerService providerService = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService();
        ControllerService providerPoolService = context.getProperty(AWS_CREDENTIALS_PROVIDER_POOL_SERVICE).asControllerService();
        if (providerService != null) {
            getLogger().debug("Using aws credentials provider service for creating client");
            onScheduledUsingControllerService(context);
        } else if (providerPoolService != null) {
            getLogger().debug("Using aws credentials provider pool service for creating client");
            onScheduledUsingControllerPoolService(context);
        } else {
            getLogger().debug("Using aws credentials for creating client");
            super.onScheduled(context);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        final boolean providerSet = validationContext.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).isSet();
        final boolean providerPoolSet = validationContext.getProperty(AWS_CREDENTIALS_PROVIDER_POOL_SERVICE).isSet();
        if (providerSet && providerPoolSet) {
            problems.add(new ValidationResult.Builder().input("Credentials Service").valid(false).explanation("Cannot set both CredentialsProviderService and CredentialsProviderPoolService").build());
        }

        return problems;
    }

    /**
     * Create aws client using credentials provider
     * @param context the process context
     */
    protected void onScheduledUsingControllerService(ProcessContext context) {
        this.client = createClient(context, getCredentialsProvider(context), createConfiguration(context));
        super.initializeRegionAndEndpoint(context);
    }

    /**
     * Creates instance of the credentials pool provider
     */
    protected void onScheduledUsingControllerPoolService(ProcessContext context) {
        providerPool = context.getProperty(AWS_CREDENTIALS_PROVIDER_POOL_SERVICE).asControllerService(AWSCredentialsProviderPoolService.class).getCredentialsProviderPool();
    }

    /**
     * Create aws client using credentials provider
     * @param flowFile the flow file to evaluate attributes
     */
    protected ClientType getClient(final ProcessContext context, final FlowFile flowFile) {
        if (providerPool != null) {
            this.client = createClient(context, providerPool.getCredentialsProvider(flowFile), createConfiguration(context));
            super.initializeRegionAndEndpoint(context);
        }

        return this.client;
    }

    @OnShutdown
    public void onShutDown() {
        if ( this.client != null ) {
           this.client.shutdown();
        }
    }

    /**
     * Get credentials provider using the {@link AWSCredentialsProviderService}
     * @param context the process context
     * @return AWSCredentialsProvider the credential provider
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    protected AWSCredentialsProvider getCredentialsProvider(final ProcessContext context) {
        return context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService(AWSCredentialsProviderService.class).getCredentialsProvider();
    }

    /**
     * Abstract method to create aws client using credentials provider.  This is the preferred method
     * for creating aws clients
     * @param context process context
     * @param credentialsProvider aws credentials provider
     * @param config aws client configuration
     * @return ClientType the client
     */
    protected abstract ClientType createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config);
}