package com.eucalyptus.tests.awssdk

import com.amazonaws.AmazonServiceException
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.ec2.AmazonEC2
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest
import com.amazonaws.services.ec2.model.CreateTagsRequest
import com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult
import com.amazonaws.services.ec2.model.Tag as Ec2Tag
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancing
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient
import com.amazonaws.services.elasticloadbalancing.model.*
import com.amazonaws.services.elasticloadbalancing.model.Tag as ElbTag
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.S3ClientOptions
import com.amazonaws.services.s3.model.BucketTaggingConfiguration
import com.amazonaws.services.s3.model.TagSet
import com.github.sjones4.youcan.youtag.YouTag
import com.github.sjones4.youcan.youtag.YouTagClient
import org.testng.annotations.Test

import static N4j.minimalInit
import static N4j.CLC_IP
import static N4j.ACCESS_KEY
import static N4j.SECRET_KEY

/**
 * Tests tag service functionality.
 *
 * Related issues:
 *   https://eucalyptus.atlassian.net/browse/EUCA-12615
 */
class TestTags {

  private final String host;
  private final AWSCredentialsProvider credentials;

  public static void main( String[] args ) throws Exception {
    new TestTags( ).tagsTest( )
  }

  public TestTags( ) {
    minimalInit()
    this.host = CLC_IP
    this.credentials = new AWSStaticCredentialsProvider( new BasicAWSCredentials( ACCESS_KEY, SECRET_KEY ) )
  }

  private String cloudUri( String servicePath ) {
    URI.create( "http://" + host + ":8773/" )
        .resolve( servicePath )
        .toString()
  }

  private AmazonEC2 getEC2Client( final AWSCredentialsProvider credentials ) {
    final AmazonEC2 ec2 = new AmazonEC2Client( credentials )
    ec2.setEndpoint( cloudUri( "/services/compute" ) )
    ec2
  }

  private AmazonElasticLoadBalancing getELBClient( final AWSCredentialsProvider credentials ) {
    final AmazonElasticLoadBalancing elb = new AmazonElasticLoadBalancingClient( credentials )
    elb.setEndpoint( cloudUri( "/services/LoadBalancing" ) )
    elb
  }

  private AmazonS3 getS3Client( final AWSCredentialsProvider credentials ) {
    final AmazonS3Client s3 = new AmazonS3Client(
        credentials,
        new ClientConfiguration( signerOverride: 'S3SignerType' )
    )
    s3.setEndpoint( cloudUri( '/services/objectstorage' ) )
    s3.setS3ClientOptions( S3ClientOptions.builder( ).setPathStyleAccess( true ).build( ) )
    s3
  }

  private YouTag getTagClient(final AWSCredentialsProvider credentials, String signerOverride = null ) {
    final YouTagClient tag = new YouTagClient(
        credentials,
        signerOverride ? new ClientConfiguration( signerOverride: signerOverride ) : new ClientConfiguration( )
    )
    tag.setEndpoint( cloudUri( '/services/Tag' ) )
    tag
  }

  @Test
  public void tagsTest( ) throws Exception {
    final AmazonEC2 ec2 = getEC2Client( credentials )

    // Find an AZ to use
    final DescribeAvailabilityZonesResult azResult = ec2.describeAvailabilityZones();

    N4j.assertThat( azResult.getAvailabilityZones().size() > 0, "Availability zone not found" );

    final String availabilityZone = azResult.getAvailabilityZones().get( 0 ).getZoneName();
    N4j.print( "Using availability zone: " + availabilityZone );

    final String namePrefix = UUID.randomUUID().toString().substring(0, 13) + "-";
    N4j.print( "Using resource prefix for test: " + namePrefix );

    final List<Runnable> cleanupTasks = [] as List<Runnable>
    try {
      N4j.print( 'Making tag service request with unsupported signature version' )
      getTagClient(credentials, 'QueryStringSignerType').getTagKeys()
      N4j.assertThat( false, 'Expected error due to request with unsupported signature version' )
    } catch ( AmazonServiceException e ) {
      N4j.print( "Exception for request with invalid signature version: ${e}" )
      N4j.assertThat(
          (e.message?:'').contains( 'ignature version not supported' ),
          'Expected failure due to signature version' )
    }

    try {
      ec2.with {
        String securityGroupName = "${namePrefix}group1"
        N4j.print( "Creating security group: ${securityGroupName}" )
        String groupId = createSecurityGroup( new CreateSecurityGroupRequest(
            groupName: securityGroupName,
            description: 'tag test group'
        ) ).with {
          groupId
        }
        N4j.print( "Created security group: ${groupId}" )
        cleanupTasks.add{
          N4j.print( "Deleting security group: ${securityGroupName}/${groupId}" )
          deleteSecurityGroup( new DeleteSecurityGroupRequest( groupId: groupId ) )
        }
        N4j.print( "Tagging security group: ${groupId}" )
        createTags( new CreateTagsRequest(
            resources: [ groupId ],
            tags: [
              new Ec2Tag( key: 'three', value: 'ec2-3' ),
              new Ec2Tag( key: 'four', value: '4' ),
              new Ec2Tag( key: 'five', value: '5' )
            ]
        ) )
      }

      getELBClient( credentials ).with {
        String loadBalancerName = "${namePrefix}balancer1"
        N4j.print( "Creating load balancer with tags: ${loadBalancerName}" )
        createLoadBalancer( new CreateLoadBalancerRequest(
            loadBalancerName: loadBalancerName,
            listeners: [ new Listener(
                loadBalancerPort: 9999,
                protocol: 'HTTP',
                instancePort: 9999,
                instanceProtocol: 'HTTP'
            ) ],
            availabilityZones: [ availabilityZone ],
            tags: [
                new ElbTag( key: 'one', value: '1' ),
                new ElbTag( key: 'two', value: '2' ),
                new ElbTag( key: 'three', value: '3' ),
            ]
        ) )
        cleanupTasks.add {
          N4j.print( "Deleting load balancer: ${loadBalancerName}" )
          deleteLoadBalancer( new DeleteLoadBalancerRequest( loadBalancerName: loadBalancerName ) )
        }
      }

      getS3Client( credentials ).with {
        String bucketName = "${namePrefix}bucket1"
        N4j.print( "Creating bucket: ${bucketName}" )
        createBucket( bucketName )
        cleanupTasks.add{
          N4j.print( "Deleting bucket: ${bucketName}" )
          deleteBucket( bucketName )
        }

        N4j.print( "Tagging bucket: ${bucketName}" )
        setBucketTaggingConfiguration( bucketName, new BucketTaggingConfiguration(
            tagSets: [
                new TagSet( [
                    'five': '5',
                    'six': '6',
                    'seven': '7'
                ] )
            ]
        ) )
      }

      getTagClient( credentials ).with {
        N4j.print( 'Getting tag keys' )
        getTagKeys( ).with {
          N4j.print( "Got tag keys: ${tagKeys}" )
          final Set<String> tagKeySet = new TreeSet<>( )
          tagKeySet.addAll( tagKeys )

          Set<String> expectedTagKeys = [ 'one', 'two', 'three', 'four', 'five', 'six', 'seven' ]
          N4j.print( "Checking for expected tag keys: ${expectedTagKeys}" )
          N4j.assertThat(
              tagKeySet.containsAll(expectedTagKeys),
              "Expected all keys: ${expectedTagKeys}"
          )
        }
      }

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
