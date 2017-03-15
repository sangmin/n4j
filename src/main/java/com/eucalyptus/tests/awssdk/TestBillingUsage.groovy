package com.eucalyptus.tests.awssdk

import java.text.SimpleDateFormat
import java.text.DateFormat
import com.amazonaws.AmazonServiceException
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.github.sjones4.youcan.youbill.YouBill
import com.github.sjones4.youcan.youbill.YouBillClient
import com.github.sjones4.youcan.youec2reports.YouEc2Reports
import com.github.sjones4.youcan.youec2reports.YouEc2ReportsClient
import com.github.sjones4.youcan.youec2reports.model.InstanceUsageFilter
import com.github.sjones4.youcan.youec2reports.model.InstanceUsageFilters
import com.github.sjones4.youcan.youec2reports.model.InstanceUsageGroup
import com.github.sjones4.youcan.youec2reports.model.ViewInstanceUsageReportRequest
import com.github.sjones4.youcan.youbill.model.ViewMonthlyUsageRequest
import com.github.sjones4.youcan.youec2reports.model.ViewReservedInstanceUtilizationReportRequest
import com.github.sjones4.youcan.youbill.model.ViewUsageRequest
import com.google.common.collect.Lists
import org.testng.annotations.Test

import static com.eucalyptus.tests.awssdk.N4j.ACCESS_KEY
import static com.eucalyptus.tests.awssdk.N4j.CLC_IP
import static com.eucalyptus.tests.awssdk.N4j.SECRET_KEY
import static com.eucalyptus.tests.awssdk.N4j.minimalInit

class TestBillingUsage {
    private final String host;
    private final AWSCredentialsProvider credentials;

    public static void main( String[] args ) throws Exception {
        new TestBillingUsage( ).billingServiceUsageTest( )
    }

    public TestBillingUsage( ) {
        minimalInit( )
        this.host = CLC_IP
        this.credentials = new AWSStaticCredentialsProvider( new BasicAWSCredentials( ACCESS_KEY, SECRET_KEY ) )
    }

    private String cloudUri( String servicePath ) {
        URI.create( "http://" + host + ":8773/" )
                .resolve( servicePath )
                .toString()
    }

    private YouBill getYouBillClient(final AWSCredentialsProvider credentials, String signerOverride = null ) {
        final YouBillClient bill = new YouBillClient(
                credentials,
                signerOverride ? new ClientConfiguration( signerOverride: signerOverride ) : new ClientConfiguration( )
        )
        bill.setEndpoint( cloudUri( '/services/Portal' ) )
        bill
    }


    private YouEc2Reports getYouEc2ReportsClient(final AWSCredentialsProvider credentials, String signerOverride = null ) {
        final YouEc2ReportsClient ec2reports = new YouEc2ReportsClient(
                credentials,
                signerOverride ? new ClientConfiguration( signerOverride: signerOverride ) : new ClientConfiguration( )
        )
        ec2reports.setEndpoint( cloudUri( '/services/Ec2Reports' ) )
        ec2reports
    }

    @Test
    public void billingServiceUsageTest( ) throws Exception {
        final List<Runnable> cleanupTasks = [] as List<Runnable>
        // verify that signature v2 requests are rejected
        try {
            N4j.print( 'Making portal service request with unsupported signature version' )
            getYouBillClient(credentials, 'QueryStringSignerType').viewUsage( new ViewUsageRequest( ) )
            N4j.assertThat( false, 'Expected error due to request with unsupported signature version' )
        } catch ( AmazonServiceException e ) {
            N4j.print( "Exception for request with invalid signature version: ${e}" )
            N4j.assertThat(
                    (e.message?:'').contains( 'Signature version not supported' ),
                    'Expected failure due to signature version' )
        }

        try {
            N4j.print( 'Making ec2reports service request with unsupported signature version' )
            getYouEc2ReportsClient(credentials, 'QueryStringSignerType').viewInstanceUsageReport( new ViewInstanceUsageReportRequest( ) )
            N4j.assertThat( false, 'Expected error due to request with unsupported signature version' )
        } catch ( AmazonServiceException e ) {
            N4j.print( "Exception for request with invalid signature version: ${e}" )
            N4j.assertThat(
                    (e.message?:'').contains( 'Signature version not supported' ),
                    'Expected failure due to signature version' )
        }

        final AWSCredentialsProvider userCred = new AWSStaticCredentialsProvider( new BasicAWSCredentials( "AKIAAAEUCQK6QYUSAHFL", "gZMTHFl5zqkIOV6d5MhzyrF756JNkVdS2hytHqMC" ) )

        try{
            final DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
            getYouBillClient( userCred ).with {
                N4j.print('Calling viewUsage')
                viewUsage(new ViewUsageRequest(
                        services: 'ec2',
                        usageTypes: 'all',
                        operations: 'all',
                        reportGranularity: 'hourly',
                        timePeriodFrom: df.parse("11/15/2016"),
                        timePeriodTo: df.parse("3/16/2017")
                )
                ).with {
                    N4j.print("Hourly usage data: ${it}")
                }

                viewUsage(new ViewUsageRequest(
                        services: 'ec2',
                        usageTypes: 'all',
                        operations: 'all',
                        reportGranularity: 'daily',
                        timePeriodFrom: df.parse("11/15/2016"),
                        timePeriodTo: df.parse("3/16/2017")
                )
                ).with {
                    N4j.print("Daily usage data: ${it}")
                }

                viewUsage(new ViewUsageRequest(
                        services: 'ec2',
                        usageTypes: 'all',
                        operations: 'all',
                        reportGranularity: 'monthly',
                        timePeriodFrom: df.parse("11/15/2016"),
                        timePeriodTo: df.parse("3/16/2017")
                )
                ).with {
                    N4j.print("Monthly usage data: ${it}")
                }


                N4j.print('Calling viewMonthlyUsage')
                viewMonthlyUsage(new ViewMonthlyUsageRequest(
                        year: '2017',
                        month: '3'
                )).with {
                    N4j.print("View monthly usage data: ${it}")
                } 


                viewMonthlyUsage(new ViewMonthlyUsageRequest(
                        year: '2017',
                        month: '2'
                )).with {
                    N4j.print("View monthly usage data: ${it}")
                }
 
                viewMonthlyUsage(new ViewMonthlyUsageRequest(
                        year: '2017',
                        month: '13'
                )).with {
                    N4j.print("View monthly usage data: ${it}")
                } 
            }

            final long MillisecondsInDay = 24*60*60*1000;
            /*getYouEc2ReportsClient( credentials ).with {
                N4j.print('Calling ec2reports::viewInstanceUsageReport')
                viewInstanceUsageReport(new ViewInstanceUsageReportRequest(
                        granularity: 'Daily',
                        timeRangeStart: (new Date(System.currentTimeMillis() - 365 * MillisecondsInDay )),
                        timeRangeEnd: new Date(System.currentTimeMillis()),
                        groupBy: new InstanceUsageGroup(
                                type: 'Availability Zone',
                                key: null),
                        filters: new InstanceUsageFilters(
                                member: Lists.newArrayList(
                                        new InstanceUsageFilter(
                                                type: 'InstanceType',
                                                key: 'm1.small'
                                        ),
                                        new InstanceUsageFilter(
                                                type: 'Tag',
                                                key: 'Group',
                                                value: 'QA'
                                        ),
                                        new InstanceUsageFilter(
                                                type: 'Platforms',
                                                key: 'Linux/Unix'
                                        ), 
                                        new InstanceUsageFilter(
                                                type: 'Platforms',
                                                key: 'Windows'
                                        ) 
                                )
                        )
                ))
                .with {
                    N4j.print("View instance usage report: ${it}")
                }
                N4j.print('Calling ec2reports::viewReservedInstanceUtilizationReport')
                viewReservedInstanceUtilizationReport(new ViewReservedInstanceUtilizationReportRequest())
                .with {
                    N4j.print("View reserved instance utilization report: ${it}")
                    N4j.print("utialization report: ${ getUtilizationReport() }")
                }
            }*/
            N4j.print( "Test complete" )
        } finally {
            // Attempt to clean up anything we created
            cleanupTasks.reverseEach { Runnable cleanupTask ->
                try {
                    cleanupTask.run()
                } catch ( Exception e ) {
                    e.printStackTrace()
                }
            }
        }
    }
}
