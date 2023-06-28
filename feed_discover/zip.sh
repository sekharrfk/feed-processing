rm -rf package
rm -rf my_deployment_package.zip

mkdir package
pip install --target ./package -r requirements.txt
cd package
zip -r ../my_deployment_package.zip .
cd ..
zip my_deployment_package.zip lambda_function.py
aws s3 cp my_deployment_package.zip s3://rfk-dataplatform-dev/my_deployment_package.zip