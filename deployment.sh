#!/bin/sh

export ENV=dev

. deployment/${ENV}/variables.sh

## Build image locally
docker buildx build --platform linux/amd64 -t ${REPO_NAME}:${VERSION} .

## Tag and Push Image to ECR
#1. Create Repo if not exists
aws ecr describe-repositories --repository-names ${REPO_NAME} || aws ecr create-repository --repository-name ${REPO_NAME}

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}

#2. Tag Image
docker tag ${REPO_NAME}:${VERSION}  ${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${VERSION}

#3. Push Image
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${VERSION}

## Create the Env Namespace
kubectl describe namespace ${ENV} || kubectl create namespace ${ENV}

## Create Service Accounts using eksctl or kubectl
if [ -z ${SERVICE_ACCOUNT_POLICY} ]
then
      kubectl describe serviceaccount ${SERVICE_ACCOUNT_NAME} -n ${ENV} || kubectl create serviceaccount ${SERVICE_ACCOUNT_NAME} -n ${ENV}
else
      eksctl create iamserviceaccount --cluster ${CLUSTER_NAME} --namespace ${ENV} --name ${SERVICE_ACCOUNT_NAME} --attach-policy-arn ${SERVICE_ACCOUNT_POLICY} --role-name ${REPO_NAME}-${ENV}-ROLE --override-existing-serviceaccounts --approve
fi

## Role and Role bindings if required kubec
if [ -f deployment/role.yaml ]; then
    kubectl apply -f deployment/role.yaml -n ${ENV}
    kubectl apply -f deployment/rolebinding.yaml -n ${ENV}
fi


## Deployment
kubectl apply -f deployment/deployment.yaml -n ${ENV}

## Service
kubectl apply -f deployment/service.yaml -n ${ENV}
