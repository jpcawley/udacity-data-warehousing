import configparser
import boto3
import botocore
import json
import time
import create_tables
import etl
import validate

config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
HOST = config.get('CLUSTER', 'HOST')

DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

DB_NAME = config.get("CLUSTER","DB_NAME")
DB_USER = config.get("CLUSTER","DB_USER")
DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
DB_PORT = config.get("CLUSTER","DB_PORT")
SECURITY_GROUP_ID = config.get("CLUSTER","SECURITY_GROUP_ID")


def createClients():
    '''Create clients for ec2, s3, iam, redshift'''
    ec2 = boto3.resource('ec2')
    s3 = boto3.resource('s3')
    iam = boto3.client('iam')
    redshift = boto3.client('redshift')
    return ec2, s3, iam, redshift

def createIamRole(iam):
    '''Create IAM role.
        parameters:
            iam: iam client'''
    try:
        print('Creating a new IAM Role,', end='')
        sparkifyRole = iam.create_role(
            Path='/',
            RoleName=ARN,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
        )
        iam.attach_role_policy(RoleName=ARN,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                               )['ResponseMetadata']['HTTPStatusCode']
        roleArn = iam.get_role(RoleName=ARN)['Role']['Arn']

        print('IAM Role created successfully.\n')
        return roleArn
    except Exception as e:
        print(e)

def createRedshiftCluster(iam, redshift):
    '''Create a redshift cluster named sparkifycluster
        parameters:
            iam: iam client
            redshift: redshift client
    '''
    roleArn = iam.get_role(RoleName=ARN)['Role']['Arn']

    try:
        response = redshift.create_cluster(        

            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            DBName=DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,

            IamRoles=[roleArn],
            
            VpcSecurityGroupIds= [SECURITY_GROUP_ID]
        )
    except Exception as e:
            print(e)

    status = response['Cluster']['ClusterStatus']

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    time.sleep(20)
    print('Cluster creation is still in progress, checking status every 10 seconds.')
    while True:
        if status == 'creating':
            time.sleep(10)
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            status = myClusterProps['ClusterStatus']
        elif status == 'available':
            sparkifyHost = myClusterProps['Endpoint']['Address']
            print('Created Redshift cluster "{}".'.format(DWH_CLUSTER_IDENTIFIER))
            break

## Clean up AWS resources:

# Delete IAM Role
def deleteIamResources(iamRole, iam):
    '''Delete all created IAM resources.
        parameters:
            iam: iam client
            iamRole: IAM role from dwh.cfg'''
    iam.detach_role_policy(RoleName=iamRole, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=iamRole)
    print('Deleted IAM Role Resources.\n')
    return

# Delete redshift cluster
def deleteRedshiftResources(redshift):
    '''Delete created Redshift resources.
        parameters:
            redshift: redshift client'''
    try:
        print('Cluster deletion in progress, checking status every 10 seconds.')
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
        while True:
            time.sleep(10)
            if not redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER):
                break 
  
    except Exception:
    # except botocore.errorfactory.ClusterNotFoundFault:
    # except botocore.exceptions.ClientError:
        print('The cluster "{}" no longer exists.'.format(DWH_CLUSTER_IDENTIFIER))
        
def main():
    '''Main method to run ETL for Sparkify teams analysis.'''
    # create clients
    ec2, s3, iam, redshift = createClients()
    
    # Create IAM Role
    createIamRole(iam)
    
    # Create Redshift Cluster
    print('Creating cluster...')
    createRedshiftCluster(iam, redshift)
    print()
    
    # Drop and create tables in Postgres
    print('Setting up database...')
    create_tables.main()
    print('Dropped and recreated the database tables.\n')
    
    # Run main ETL
    print('Starting transform and load processes.\n')
    etl.main()
    print('All data has been extracted, staged, transformed, and loaded into postgres. Main ETL complete.\n')
    
    try:
        test = str(input('Would you like to validate the data? (Y/N)')).upper()
    except ValueError:
        print('Invalid input. Please enter Y or N.\n')
        
    if test == 'Y':
        validate.main()
    else:
        print("You've elected not to (re)validate the data; as a result, all AWS resources are being deleted.\n")
            
        deleteIamResources(ARN, iam)      
        deleteRedshiftResources(redshift)
    
if __name__ == "__main__":
    main()
