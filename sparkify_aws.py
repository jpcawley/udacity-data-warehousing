import configparser
import boto3
import botocore
import json
import time
import create_tables

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

# Create Clients
s3 = boto3.resource('s3')
iam = boto3.client('iam')
redshift = boto3.client('redshift')

def createIamRole():
    '''Create IAM role.'''
    try:
        print('1.1 Creating a new IAM Role')
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
        print('1.1 Attaching a Policy')
        iam.attach_role_policy(RoleName=ARN,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                               )['ResponseMetadata']['HTTPStatusCode']
        print('1.3 Get the IAM role ARN')
        roleArn = iam.get_role(RoleName=ARN)['Role']['Arn']

        print('IAM Role created successfully.')
        return roleArn
    except Exception as e:
        print(e)

def createRedshiftCluster():
    roleArn = iam.get_role(RoleName=ARN)['Role']['Arn']
    print('Creating cluster "{}".'.format(DWH_CLUSTER_IDENTIFIER))
    try:
        response = redshift.create_cluster(        

            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            DBName=DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,

            IamRoles=[roleArn] 
        )
    except Exception as e:
            print(e)

    status = response['Cluster']['ClusterStatus']

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print(myClusterProps)
    time.sleep(20)
    print('Cluster has not finished creating yet.')
    while True:
        if status == 'creating':
            time.sleep(20)
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            status = myClusterProps['ClusterStatus']
        elif status == 'available':
            sparkifyHost = myClusterProps['Endpoint']['Address']
            print('sparkifyHost: ', sparkifyHost)
            print('Created Redshift cluster "{}".'.format(DWH_CLUSTER_IDENTIFIER))
            break

def openTCP():
    try:
        vpcId = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['VpcId']
        vpc = ec2.Vpc(id=vpcId)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
            )
    except Exception as e:
        print(e)
            
def deleteIamResources(iamRole):
    '''Delete all created IAM resources.'''
    iam.detach_role_policy(RoleName=iamRole, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=iamRole)
    print('Deleted IAM Role Resources.')
    return

def deleteRedshiftResources(iamRole):
    '''Delete created Redshift resources.'''
    try:
        print('Cluster deletion in progress.')
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
        while True:
            time.sleep(20)
            if not redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER):
                break     
    except Exception:
    # except botocore.exceptions.ClientError:
        print('The cluster "{}" no longer exists.'.format(DWH_CLUSTER_IDENTIFIER))

def main():
    # CREATE IAM ROLE
#     createIamRole()
    
    # CREATE REDSHIFT CLUSTER
#     createRedshiftCluster()

    # OPEN INCOMING TCP PORT
#     openTCP()
    
    # DROP AND CREATE TABLES
#     create_tables.main()
    
    # DELETE CREATED RESOURCES
    deleteIamResources(ARN)
    deleteRedshiftResources(ARN)
    print('All resources successfully deleted.')
    
    ## UNCOMMENT TO SEE BUCKET CONTENTS FOR LOG-DATA
    # print('Getting bucket from config')
    # bckt = config.get('S3','UDACITY_BUCKET')

    # print('Trying to get bucket using s3 client')
    # songbucket = createS3Client().Bucket(bckt)

    # print('Trying to iterate through bucket objects')
    # for obj in songbucket.objects.filter(Prefix='log-data/'):
    #     print(obj)

if __name__ == "__main__":
    main()
