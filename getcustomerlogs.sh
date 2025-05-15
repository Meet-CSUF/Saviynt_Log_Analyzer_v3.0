#!/bin/bash
echo "Enter arn name:"
read arn_name
echo "Enter MFA Auth Token Code"
read token_code
credentials_json=$(aws sts get-session-token --serial-number arn:aws:iam::854587915883:mfa/$arn_name --token-code $token_code)
aws_access_key=$(echo "$credentials_json" | jq -r '.Credentials.AccessKeyId')
aws_secret_access_key=$(echo "$credentials_json" | jq -r '.Credentials.SecretAccessKey')
aws_session_token=$(echo "$credentials_json" | jq -r '.Credentials.SessionToken')
export AWS_ACCESS_KEY_ID=$aws_access_key
export AWS_SECRET_ACCESS_KEY=$aws_secret_access_key
export AWS_SESSION_TOKEN=$aws_session_token
echo "Enter Appropriate Role Assume:"
read role_assume
echo "Enter MFA Auth Token Code"
read token_code
credentials_json=$(aws sts assume-role --role-arn arn:aws:iam::854587915883:role/$role_assume --role-session-name $arn_name --serial-number arn:aws:iam::854587915883:mfa/$arn_name --token-code $token_code)
aws_access_key=$(echo "$credentials_json" | jq -r '.Credentials.AccessKeyId')
aws_secret_access_key=$(echo "$credentials_json" | jq -r '.Credentials.SecretAccessKey')
aws_session_token=$(echo "$credentials_json" | jq -r '.Credentials.SessionToken')
export AWS_ACCESS_KEY_ID=$aws_access_key
export AWS_SECRET_ACCESS_KEY=$aws_secret_access_key
export AWS_SESSION_TOKEN=$aws_session_token 

get_caller_identity=$(aws sts get-caller-identity)
echo get_caller_identity


# Date increment function using Python (cross-platform and reliable)
increment_date() {
    python3 -c "from datetime import datetime, timedelta; print((datetime.strptime('$1', '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d'))"
}

# Prompt for input
read -p "Enter customer name: " customer
read -p "Enter start date (YYYYMMDD): " start_date
read -p "Enter end date (YYYYMMDD, leave blank for single day): " end_date
read -p "Enter S3 bucket name: " bucket

# Handle hour range
if [ -z "$end_date" ]; then
    end_date="$start_date"
    read -p "Enter start hour (00-23): " start_hour
    read -p "Enter end hour (00-23): " end_hour
    start_hour=$(printf "%02d" "$start_hour")
    end_hour=$(printf "%02d" "$end_hour")
else
    start_hour="00"
    end_hour="23"
fi

current_date="$start_date"

while [[ "$current_date" -le "$end_date" ]]; do
    # For a single day, use user-specified hour range; for multiple days, use all hours
    if [[ "$start_date" == "$end_date" ]]; then
        hour_start="$start_hour"
        hour_end="$end_hour"
    else
        hour_start="00"
        hour_end="23"
    fi

    for hour in $(seq -w $hour_start $hour_end); do
        s3_prefix="${customer}/${current_date}-${hour}/"
        local_dir="./customer_logs/${current_date}-${hour}/"
        echo "Fetching logs from: s3://${bucket}/${s3_prefix} to ${local_dir}"
        mkdir -p "$local_dir"
        aws s3 cp "s3://${bucket}/${s3_prefix}" "$local_dir" --recursive
    done

    if [[ "$current_date" == "$end_date" ]]; then
        break
    fi
    current_date=$(increment_date "$current_date")
done

echo "✅ All logs downloaded to ./customer_logs/${customer}/"

